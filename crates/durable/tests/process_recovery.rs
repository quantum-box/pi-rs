//! Recovery after an OS-killed scheduler, including release of its owner lock.
#![cfg(unix)]

use std::{
    io::{BufRead, BufReader, Write},
    process::{Child, Command, Stdio},
    sync::mpsc,
    time::Duration,
};

use pi_rs_durable::{
    task::{Checkpoint, Task, TaskOwner, TaskState, WaitFor},
    task_scheduler::{Error, Registry, Scheduler, Step},
};
use serde_json::json;
use uuid::Uuid;

const TASK: Uuid = Uuid::from_u128(1);
const OWNER: Uuid = Uuid::from_u128(2);
const FAST_CHILD: Uuid = Uuid::from_u128(3);
const SLOW_CHILD: Uuid = Uuid::from_u128(4);

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

#[test]
fn held_outcome_child() {
    let Ok(path) = std::env::var("PI_HELD_CHILD_DB") else {
        return;
    };
    let fail = std::env::var_os("PI_HELD_FAIL").is_some();
    let mut registry = Registry::default();
    registry.register("parent", true, move |task: &Task| {
        if task.checkpoint().phase == "first" {
            let child = |id, kind: &str| {
                Task::new(id, TaskOwner::Task(TASK), checkpoint("first"), 3)
                    .unwrap()
                    .registered(kind.into(), true)
            };
            Ok(Step::Wait {
                checkpoint: checkpoint("second"),
                condition: WaitFor::TasksAny(vec![FAST_CHILD, SLOW_CHILD]),
                children: vec![child(FAST_CHILD, "fast"), child(SLOW_CHILD, "slow")],
            })
        } else if fail {
            Ok(Step::Fail("held failure".into()))
        } else {
            Ok(Step::Succeed(json!("held success")))
        }
    });
    registry.register("fast", true, |_: &Task| Ok(Step::Succeed(json!(null))));
    registry.register_with_abort(
        "slow",
        true,
        |_: &Task| panic!("slow child ran before hold"),
        |_: &Task| Ok(Step::Succeed(json!(null))),
    );
    let mut scheduler = Scheduler::open(path, registry).unwrap();
    let root = Task::new(TASK, TaskOwner::Conversation(OWNER), checkpoint("first"), 3)
        .unwrap()
        .registered("parent".into(), true);
    scheduler.submit(&root).unwrap();
    for _ in 0..3 {
        assert!(scheduler.tick(0).unwrap());
    }
    let state = scheduler.get(TASK).unwrap().unwrap();
    assert!(matches!(
        state.state(),
        TaskState::Completing(_) | TaskState::CompletingFailed(_)
    ));
    println!("CHILD_HELD_OUTCOME");
    std::io::stdout().flush().unwrap();
    loop {
        std::thread::park();
    }
}

#[test]
fn held_outcomes_resume_after_process_death() {
    for fail in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tasks.db");
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args(["--exact", "held_outcome_child", "--nocapture"])
            .env("PI_HELD_CHILD_DB", &path)
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit());
        if fail {
            command.env("PI_HELD_FAIL", "1");
        } else {
            command.env_remove("PI_HELD_FAIL");
        }
        let mut child = KillOnDrop(command.spawn().unwrap());
        let stdout = child.0.stdout.take().unwrap();
        let (tx, rx) = mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                if line.unwrap().contains("CHILD_HELD_OUTCOME") {
                    let _ = tx.send(());
                    break;
                }
            }
        });
        rx.recv_timeout(Duration::from_secs(20))
            .expect("held outcome not committed");
        child.0.kill().unwrap();
        assert!(!child.0.wait().unwrap().success());
        let mut registry = Registry::default();
        registry.register_with_abort(
            "slow",
            true,
            |_: &Task| Ok(Step::Succeed(json!("done"))),
            |_: &Task| Ok(Step::Succeed(json!(null))),
        );
        let mut scheduler = Scheduler::open(&path, registry).unwrap();
        for _ in 0..5 {
            if scheduler.get(TASK).unwrap().unwrap().is_terminal() {
                break;
            }
            assert!(scheduler.tick(0).unwrap());
        }
        let root = scheduler.get(TASK).unwrap().unwrap();
        if fail {
            assert_eq!(root.state(), &TaskState::Failed("held failure".into()));
            assert_eq!(
                scheduler.get(SLOW_CHILD).unwrap().unwrap().state(),
                &TaskState::Cancelled
            );
        } else {
            assert_eq!(root.state(), &TaskState::Succeeded(json!("held success")));
            assert_eq!(
                scheduler.get(SLOW_CHILD).unwrap().unwrap().state(),
                &TaskState::Succeeded(json!("done"))
            );
        }
    }
}

#[test]
fn external_wait_child() {
    let Ok(path) = std::env::var("PI_EXTERNAL_WAIT_DB") else {
        return;
    };
    let mut registry = Registry::default();
    registry.register("waiter", true, |_: &Task| {
        Ok(Step::Wait {
            checkpoint: checkpoint("second"),
            condition: WaitFor::TasksAny(vec![FAST_CHILD]),
            children: vec![],
        })
    });
    registry.register("other", true, |_: &Task| {
        panic!("other must not run before kill")
    });
    let mut scheduler = Scheduler::open(path, registry).unwrap();
    let waiter = Task::new(TASK, TaskOwner::Conversation(OWNER), checkpoint("first"), 3)
        .unwrap()
        .registered("waiter".into(), true);
    let other = Task::new(
        FAST_CHILD,
        TaskOwner::Conversation(OWNER),
        checkpoint("first"),
        3,
    )
    .unwrap()
    .registered("other".into(), true);
    scheduler.submit(&waiter).unwrap();
    scheduler.submit(&other).unwrap();
    scheduler.tick(0).unwrap();
    assert_eq!(
        scheduler.get(TASK).unwrap().unwrap().state(),
        &TaskState::Waiting(WaitFor::TasksAny(vec![FAST_CHILD]))
    );
    println!("EXTERNAL_WAIT_COMMITTED");
    std::io::stdout().flush().unwrap();
    loop {
        std::thread::park();
    }
}

#[test]
fn external_wait_recovers_after_process_death() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("tasks.db");
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .args(["--exact", "external_wait_child", "--nocapture"])
        .env("PI_EXTERNAL_WAIT_DB", &path)
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit());
    let mut child = KillOnDrop(command.spawn().unwrap());
    let stdout = child.0.stdout.take().unwrap();
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            if line.unwrap().contains("EXTERNAL_WAIT_COMMITTED") {
                let _ = tx.send(());
                break;
            }
        }
    });
    rx.recv_timeout(Duration::from_secs(20))
        .expect("external wait not committed");
    child.0.kill().unwrap();
    assert!(!child.0.wait().unwrap().success());
    let mut registry = Registry::default();
    registry.register("waiter", true, |task: &Task| {
        assert_eq!(task.checkpoint(), &checkpoint("second"));
        assert_eq!(task.ended_children(), Some(&[FAST_CHILD][..]));
        Ok(Step::Succeed(json!("woken")))
    });
    registry.register("other", true, |_: &Task| Ok(Step::Succeed(json!("done"))));
    let mut scheduler = Scheduler::open(&path, registry).unwrap();
    assert!(scheduler.tick(0).unwrap());
    assert!(scheduler.tick(0).unwrap());
    assert_eq!(
        scheduler.get(TASK).unwrap().unwrap().state(),
        &TaskState::Succeeded(json!("woken"))
    );
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
            assert!(
                matches!(task.state(), TaskState::Failed(reason) if reason.contains("restart limit"))
            );
            assert!(!scheduler.tick(0).unwrap());
        }
    }
    let mut reopened = Scheduler::open(&path, Registry::default()).unwrap();
    let task = reopened.get(TASK).unwrap().unwrap();
    assert_eq!(task.checkpoint(), &checkpoint("second"));
    assert!(matches!(task.state(), TaskState::Failed(reason) if reason.contains("restart limit")));
    assert!(!reopened.tick(0).unwrap());
}

// The failure is committed while a non-replay-safe sibling is still executing.
// Killing here leaves no fail-fast mark, exactly the recovery ordering boundary.
#[test]
fn fail_fast_kill_child() {
    let Ok(path) = std::env::var("PI_FAIL_FAST_KILL_DB") else {
        return;
    };
    use durable_store::owner::OwnedStore;
    use pi_rs_durable::task_store::{Save, TaskStore};
    use std::sync::Arc;
    let owner = Arc::new(OwnedStore::open(path).unwrap());
    let mut parent = Task::new(TASK, TaskOwner::Conversation(OWNER), checkpoint("first"), 4)
        .unwrap()
        .registered("parent".into(), false);
    parent.start().unwrap();
    parent
        .wait(WaitFor::TasksFailFast(vec![FAST_CHILD, SLOW_CHILD]))
        .unwrap();
    let running = Task::new(FAST_CHILD, TaskOwner::Task(TASK), checkpoint("first"), 4)
        .unwrap()
        .registered("running".into(), false);
    let failing = Task::new(SLOW_CHILD, TaskOwner::Task(TASK), checkpoint("first"), 4)
        .unwrap()
        .registered("failing".into(), false);
    TaskStore::new(owner.store())
        .save(&[
            Save::Create(&parent),
            Save::Create(&running),
            Save::Create(&failing),
        ])
        .unwrap();
    let address = pi_rs_durable::document::Address {
        scope: "fail-fast".into(),
        name: "owned".into(),
        key: "running".into(),
    };
    let mut tx = pi_rs_durable::document::Transaction::new();
    tx.acquire_task_document(&address, FAST_CHILD).unwrap();
    tx.put(
        &address,
        durable_store::Expected::Missing,
        json!({"live": true}).as_object().unwrap(),
    )
    .unwrap();
    tx.commit(owner.store()).unwrap();
    let store_owner = owner.clone();
    let mut registry = Registry::default();
    registry.register("parent", false, |_: &Task| Ok(Step::Succeed(json!(null))));
    registry.register("running", false, move |_: &Task| {
        let store = TaskStore::new(store_owner.store());
        let sibling = store.load(SLOW_CHILD).unwrap().unwrap();
        store
            .update(&sibling, |task| {
                task.start()?;
                task.fail("failed".into())
            })
            .unwrap();
        println!("FAIL_FAST_RUNNING_AND_FAILED");
        std::io::stdout().flush().unwrap();
        loop {
            std::thread::park();
        }
    });
    let mut scheduler = Scheduler::attach(owner, registry).unwrap();
    scheduler.tick(0).unwrap();
}

#[test]
fn fail_fast_kill_before_mark_recovers_without_replaying_handler() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("tasks.db");
    let mut child = KillOnDrop(
        Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "fail_fast_kill_child", "--nocapture"])
            .env("PI_FAIL_FAST_KILL_DB", &path)
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap(),
    );
    let stdout = child.0.stdout.take().unwrap();
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            if line.unwrap().contains("FAIL_FAST_RUNNING_AND_FAILED") {
                let _ = tx.send(());
                break;
            }
        }
    });
    rx.recv_timeout(Duration::from_secs(20))
        .expect("handler did not reach failure boundary");
    let address = pi_rs_durable::document::Address {
        scope: "fail-fast".into(),
        name: "owned".into(),
        key: "running".into(),
    };
    let db = durable_store::Store::open(&path).unwrap();
    assert!(
        pi_rs_durable::document::Documents::new(&db)
            .get(&address)
            .unwrap()
            .is_some()
    );
    child.0.kill().unwrap();
    child.0.wait().unwrap();
    let mut registry = Registry::default();
    registry.register("parent", false, |_: &Task| Ok(Step::Succeed(json!("done"))));
    registry.register_with_abort(
        "running",
        false,
        |_: &Task| panic!("non-replay-safe handler replayed"),
        |_: &Task| Ok(Step::Succeed(json!(null))),
    );
    let mut scheduler = Scheduler::open(&path, registry).unwrap();
    let before = scheduler.get(FAST_CHILD).unwrap().unwrap();
    assert_eq!(before.state(), &TaskState::Running);
    assert!(!before.abort_requested());
    scheduler.tick(0).unwrap();
    let marked = scheduler.get(FAST_CHILD).unwrap().unwrap();
    assert_eq!(marked.state(), &TaskState::Running);
    assert!(marked.abort_requested());
    assert!(
        pi_rs_durable::document::Documents::new(&db)
            .get(&address)
            .unwrap()
            .is_some()
    );
    for _ in 0..8 {
        scheduler.tick(0).unwrap();
        if scheduler.get(TASK).unwrap().unwrap().is_terminal() {
            break;
        }
    }
    assert_eq!(
        scheduler.get(FAST_CHILD).unwrap().unwrap().state(),
        &TaskState::Cancelled
    );
    assert_eq!(
        scheduler.get(TASK).unwrap().unwrap().state(),
        &TaskState::Succeeded(json!("done"))
    );
    assert!(
        pi_rs_durable::document::Documents::new(&db)
            .get(&address)
            .unwrap()
            .is_none()
    );
}

#[test]
fn bounded_driver_kill_child() {
    let Ok(path) = std::env::var("PI_BOUNDED_DRIVER_KILL_DB") else {
        return;
    };
    let mut registry = Registry::default();
    registry.register("work", false, |task: &Task| {
        println!("BOUNDED_RESERVED_{}", task.id());
        std::io::stdout().flush().unwrap();
        loop {
            std::thread::park();
        }
    });
    let scheduler = Scheduler::open(path, registry).unwrap();
    for id in [1u128, 2] {
        let task = Task::new(
            Uuid::from_u128(id),
            TaskOwner::Conversation(Uuid::from_u128(id + 10)),
            checkpoint("first"),
            3,
        )
        .unwrap()
        .registered("work".into(), false);
        scheduler.submit(&task).unwrap();
    }
    let _driver = pi_rs_durable::task_driver::Driver::start(scheduler);
    loop {
        std::thread::park();
    }
}

#[test]
fn bounded_driver_kill_recovers_two_nonreplay_safe_attempts_without_replay() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut child = KillOnDrop(
        Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "bounded_driver_kill_child", "--nocapture"])
            .env("PI_BOUNDED_DRIVER_KILL_DB", &path)
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap(),
    );
    let stdout = child.0.stdout.take().unwrap();
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let mut found = std::collections::HashSet::new();
        for line in BufReader::new(stdout).lines() {
            let line = line.unwrap();
            for id in [1u128, 2] {
                if line.contains(&format!("BOUNDED_RESERVED_{}", Uuid::from_u128(id))) {
                    found.insert(id);
                }
            }
            if found.len() == 2 {
                let _ = tx.send(());
                break;
            }
        }
    });
    rx.recv_timeout(Duration::from_secs(20))
        .expect("both workers did not overlap");
    child.0.kill().unwrap();
    assert!(!child.0.wait().unwrap().success());
    let mut registry = Registry::default();
    registry.register("work", false, |_: &Task| -> Result<Step, String> {
        panic!("non-replay-safe handler reran")
    });
    let mut scheduler = Scheduler::open(&path, registry).unwrap();
    for id in [1u128, 2] {
        assert!(
            matches!(scheduler.get(Uuid::from_u128(id)).unwrap().unwrap().state(), TaskState::Failed(reason) if reason.contains("outcome requires reconciliation"))
        );
    }
    assert!(!scheduler.tick(0).unwrap());
}
