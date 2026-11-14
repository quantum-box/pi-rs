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
    if std::env::var_os("PI_RECOVERY_EXISTING").is_none() {
        let task = Task::new(TASK, TaskOwner::Conversation(OWNER), checkpoint("first"), 3)
            .unwrap()
            .registered("job".into(), replay_safe);
        scheduler.submit(&task).unwrap();
        assert!(scheduler.tick(0).unwrap());
    }
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

fn kill_during_second_invocation(path: &std::path::Path, replay_safe: bool, existing: bool) {
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .args(["--exact", "recovery_child", "--nocapture"])
        .env("PI_RECOVERY_CHILD_DB", path)
        .env(
            "PI_RECOVERY_CHILD_REPLAY",
            if replay_safe { "yes" } else { "no" },
        )
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit());
    if existing {
        command.env("PI_RECOVERY_EXISTING", "1");
    } else {
        command.env_remove("PI_RECOVERY_EXISTING");
    }
    let child = command.spawn().unwrap();
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
    kill_during_second_invocation(&path, true, false);
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
    kill_during_second_invocation(&path, false, false);
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

// Upstream's harness-tasks-recovery test resumes an intent checkpoint after
// interruption. Exercise the additional pi-rs restart budget with actual OS
// deaths after the second checkpoint is committed and each run is reserved.
#[test]
fn repeated_process_deaths_exhaust_restart_limit_without_losing_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("tasks.db");
    for attempt in 0..3 {
        kill_during_second_invocation(&path, true, attempt != 0);
        let mut registry = Registry::default();
        registry.register("job", true, |_: &Task| -> Result<Step, String> {
            panic!("opening or inspecting must not invoke the handler")
        });
        let mut scheduler = Scheduler::open(&path, registry).unwrap();
        let task = scheduler.get(TASK).unwrap().unwrap();
        assert_eq!(task.checkpoint(), &checkpoint("second"));
        assert_eq!(task.owner(), &TaskOwner::Conversation(OWNER));
        if attempt < 2 {
            assert_eq!(task.state(), &TaskState::Ready);
            assert_eq!(task.restarts(), attempt + 1);
        } else {
            assert!(matches!(task.state(), TaskState::Failed(reason) if reason.contains("restart limit")));
            assert!(!scheduler.tick(0).unwrap());
        }
    }
    let mut reopened = Scheduler::open(&path, Registry::default()).unwrap();
    let task = reopened.get(TASK).unwrap().unwrap();
    assert_eq!(task.checkpoint(), &checkpoint("second"));
    assert!(matches!(task.state(), TaskState::Failed(reason) if reason.contains("restart limit")));
    assert!(!reopened.tick(0).unwrap());
}
