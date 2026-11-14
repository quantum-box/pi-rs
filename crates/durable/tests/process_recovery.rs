//! Recovery after an OS-killed scheduler, including release of its owner lock.
#![cfg(unix)]

use std::{
    io::{BufRead, BufReader, Write},
    process::{Child, Command, Stdio},
    sync::mpsc,
    time::Duration,
};

use pi_rs_durable::{
    task::{Checkpoint, Task, TaskOwner, TaskState},
    task_scheduler::{Error, Registry, Scheduler, Step},
};
use serde_json::json;
use uuid::Uuid;

const TASK: Uuid = Uuid::from_u128(1);
const OWNER: Uuid = Uuid::from_u128(2);

fn checkpoint(phase: &str) -> Checkpoint {
    Checkpoint {
        phase: phase.into(),
        data: json!({"committed": phase == "second"}),
    }
}

// This test is launched as a separate process by the parent tests. Its handler
// announces only after the scheduler has durably reserved the second invocation.
#[test]
fn recovery_child() {
    let Ok(path) = std::env::var("PI_RECOVERY_CHILD_DB") else {
        return;
    };
    let replay_safe = std::env::var("PI_RECOVERY_CHILD_REPLAY").unwrap() == "yes";
    let mut registry = Registry::default();
    registry.register("job", replay_safe, |task: &Task| {
        if task.checkpoint().phase == "first" {
            return Ok(Step::Continue(checkpoint("second")));
        }
        assert_eq!(task.state(), &TaskState::Running);
        println!("CHILD_RESERVED_SECOND");
        std::io::stdout().flush().unwrap();
        loop {
            std::thread::park();
        }
    });
    let mut scheduler = Scheduler::open(path, registry).unwrap();
    let task = Task::new(TASK, TaskOwner::Conversation(OWNER), checkpoint("first"), 3)
        .unwrap()
        .registered("job".into(), replay_safe);
    scheduler.submit(&task).unwrap();
    assert!(scheduler.tick(0).unwrap());
    assert!(scheduler.tick(0).unwrap());
    panic!("blocked handler returned");
}

struct KillOnDrop(Child);
impl Drop for KillOnDrop {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

fn kill_during_second_invocation(path: &std::path::Path, replay_safe: bool) {
    let child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "recovery_child", "--nocapture"])
        .env("PI_RECOVERY_CHILD_DB", path)
        .env(
            "PI_RECOVERY_CHILD_REPLAY",
            if replay_safe { "yes" } else { "no" },
        )
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();
    let mut child = KillOnDrop(child);
    let stdout = child.0.stdout.take().unwrap();
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            let line = line.unwrap();
            if line.contains("CHILD_RESERVED_SECOND") {
                let _ = tx.send(());
                break;
            }
        }
    });
    rx.recv_timeout(Duration::from_secs(20))
        .expect("child did not reach reserved handler");
    assert!(matches!(
        Scheduler::open(path, Registry::default()),
        Err(Error::Owner(durable_store::owner::OwnerError::Busy))
    ));
    child.0.kill().unwrap();
    let status = child.0.wait().unwrap();
    assert!(!status.success(), "child must die while handler is running");
}

#[test]
fn replay_safe_task_resumes_committed_checkpoint_after_process_death() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("tasks.db");
    kill_during_second_invocation(&path, true);
    let mut registry = Registry::default();
    registry.register("job", true, |task: &Task| {
        assert_eq!(task.checkpoint(), &checkpoint("second"));
        assert_eq!(task.restarts(), 1);
        Ok(Step::Succeed(json!({"resumed": true})))
    });
    let mut scheduler = Scheduler::open(&path, registry).unwrap();
    let recovered = scheduler.get(TASK).unwrap().unwrap();
    assert_eq!(recovered.state(), &TaskState::Ready);
    assert_eq!(recovered.owner(), &TaskOwner::Conversation(OWNER));
    assert_eq!(recovered.checkpoint(), &checkpoint("second"));
    assert_eq!(recovered.restarts(), 1);
    assert!(scheduler.tick(0).unwrap());
    assert_eq!(
        scheduler.get(TASK).unwrap().unwrap().state(),
        &TaskState::Succeeded(json!({"resumed": true}))
    );
    drop(scheduler);
    let reopened = Scheduler::open(&path, Registry::default()).unwrap();
    assert_eq!(
        reopened.get(TASK).unwrap().unwrap().state(),
        &TaskState::Succeeded(json!({"resumed": true}))
    );
}

#[test]
fn non_replay_safe_task_fails_without_reinvoking_after_process_death() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("tasks.db");
    kill_during_second_invocation(&path, false);
    let mut registry = Registry::default();
    registry.register("job", false, |_: &Task| -> Result<Step, String> {
        panic!("uncertain handler must not replay")
    });
    let mut scheduler = Scheduler::open(&path, registry).unwrap();
    let recovered = scheduler.get(TASK).unwrap().unwrap();
    assert_eq!(recovered.checkpoint(), &checkpoint("second"));
    assert_eq!(recovered.owner(), &TaskOwner::Conversation(OWNER));
    assert!(
        matches!(recovered.state(), TaskState::Failed(reason) if reason.contains("outcome requires reconciliation"))
    );
    assert!(!scheduler.tick(0).unwrap());
    drop(scheduler);
    let reopened = Scheduler::open(&path, Registry::default()).unwrap();
    assert_eq!(reopened.get(TASK).unwrap().unwrap(), recovered);
}
