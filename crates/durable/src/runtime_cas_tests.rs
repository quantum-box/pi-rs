use super::*;
use futures_util::{StreamExt, stream};
use pi_rs_core::{ModelEvent, ModelRequest, ModelStream};

struct ModelStub;
#[async_trait::async_trait]
impl Model for ModelStub {
    async fn stream(&self, _: ModelRequest) -> Result<ModelStream, pi_rs_core::Error> {
        Ok(stream::iter(vec![Ok(ModelEvent::Complete {
            calls: vec![],
            usage: None,
        })])
        .boxed())
    }
}
fn options() -> Options {
    Options {
        model: Arc::new(ModelStub),
        tools: vec![],
        restart_limit: 3,
        max_turns: 5,
        uncertain_policy: UncertainPolicy::InterruptedResult,
    }
}
#[test]
fn cas_merges_admission_and_boundary_retries_instead_of_losing_steer() {
    let dir = tempfile::tempdir().unwrap();
    let mut admission = Harness::open_paused(dir.path().join("db"), options()).unwrap();
    let id = admission
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 10,
        })
        .unwrap();
    admission.submit(id, None, "initial".into()).unwrap();
    let mut worker = admission.worker();
    let (mut stale, version) = worker.load(id).unwrap();
    let steer = admission
        .submit_with_mode(id, Some("key".into()), "steer".into(), InputMode::Steer)
        .unwrap();
    stale.phase = Phase::Boundary {
        turns: 1,
        answer: Some("answer".into()),
    };
    worker.save(&stale, Expected::Version(version)).unwrap();
    let (mut boundary, version) = worker.load(id).unwrap();
    assert_eq!(boundary.input_modes.get(&steer), Some(&InputMode::Steer));
    let next = admission
        .submit_with_mode(id, Some("next".into()), "next".into(), InputMode::Steer)
        .unwrap();
    finish_answer(&mut boundary, "answer".into());
    worker.merge_admissions = false;
    assert!(matches!(
        worker.save(&boundary, Expected::Version(version)),
        Err(Error::Storage(durable_store::Error::Conflict { .. }))
    ));
    let (latest, _) = worker.load(id).unwrap();
    assert!(matches!(latest.phase, Phase::Boundary { .. }));
    assert_eq!(latest.submissions.len(), 3);
    assert_eq!(latest.input_modes.get(&next), Some(&InputMode::Steer));
}

#[test]
fn concurrent_duplicate_and_distinct_admissions_retry_cas() {
    let dir = tempfile::tempdir().unwrap();
    let mut owner = Harness::open_paused(dir.path().join("db"), options()).unwrap();
    let id = owner
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 10,
        })
        .unwrap();
    let barrier = Arc::new(std::sync::Barrier::new(8));
    let threads: Vec<_> = (0..8)
        .map(|i| {
            // Worker handles normally merge execution changes; admissions must
            // still retry the request-ID check on a conflict.
            let mut worker = owner.worker();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                let same = worker
                    .submit_with_mode(
                        id,
                        Some("same".into()),
                        format!("input-{i}"),
                        InputMode::Steer,
                    )
                    .unwrap();
                let unique = worker
                    .submit(id, Some(format!("unique-{i}")), format!("unique-{i}"))
                    .unwrap();
                (same, unique)
            })
        })
        .collect();
    let results: Vec<_> = threads.into_iter().map(|t| t.join().unwrap()).collect();
    assert!(results.iter().all(|r| r.0 == results[0].0));
    let state = owner.conversation(id).unwrap();
    assert_eq!(state.submissions.len(), 9);
    for (_, unique) in results {
        assert!(state.submissions.iter().any(|s| s.id == unique));
    }
}

#[test]
fn withdrawal_is_durable_and_merges_with_worker_progress() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut owner = Harness::open_paused(&path, options()).unwrap();
    let id = owner
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 10,
        })
        .unwrap();
    owner.submit(id, None, "active".into()).unwrap();
    let sid = owner
        .submit(id, Some("request".into()), "queued".into())
        .unwrap();
    let mut worker = owner.worker();
    let (mut stale, version) = worker.load(id).unwrap();
    assert_eq!(
        owner.withdraw_submission(id, sid).unwrap(),
        WithdrawResult::Aborted
    );
    stale.phase = Phase::ModelReady { turns: 0 };
    worker.save(&stale, Expected::Version(version)).unwrap();
    drop(worker);
    drop(owner);
    let mut reopened = Harness::open_paused(&path, options()).unwrap();
    assert_eq!(
        reopened.conversation(id).unwrap().submissions[1].status,
        Status::Unanswered {
            reason: "aborted".into()
        }
    );
    assert_eq!(
        reopened
            .submit(id, Some("request".into()), "duplicate".into())
            .unwrap(),
        sid
    );
    assert_eq!(
        reopened.withdraw_submission(id, sid).unwrap(),
        WithdrawResult::Settled
    );
}

#[test]
fn submissions_racing_abort_are_neither_lost_nor_duplicated() {
    let dir = tempfile::tempdir().unwrap();
    let mut owner = Harness::open_paused(dir.path().join("db"), options()).unwrap();
    let id = owner
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 10,
        })
        .unwrap();
    let barrier = Arc::new(std::sync::Barrier::new(9));
    let mut workers = Vec::new();
    for index in 0..8 {
        let mut worker = owner.worker();
        worker.merge_admissions = false;
        let barrier = barrier.clone();
        workers.push(std::thread::spawn(move || {
            barrier.wait();
            worker
                .submit(id, Some(format!("key-{index}")), format!("input-{index}"))
                .unwrap()
        }));
    }
    let mut aborter = owner.worker();
    aborter.merge_admissions = false;
    let start = barrier.clone();
    let abort = std::thread::spawn(move || {
        start.wait();
        aborter.request_abort(id).unwrap();
    });
    let ids: Vec<_> = workers
        .into_iter()
        .map(|worker| worker.join().unwrap())
        .collect();
    abort.join().unwrap();
    let state = owner.conversation(id).unwrap();
    assert_eq!(state.submissions.len(), 8);
    for sid in ids {
        let item = state
            .submissions
            .iter()
            .find(|item| item.id == sid)
            .unwrap();
        assert!(
            matches!(item.status, Status::Queued | Status::Running)
                || item.status
                    == Status::Unanswered {
                        reason: "aborted".into()
                    }
        );
    }
}
#[test]
fn admission_policies_race_and_recover_with_idempotency() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut owner = Harness::open_paused(&path, options()).unwrap();
    let id = owner
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 10,
        })
        .unwrap();
    let idle_reject = AdmissionOptions {
        reject_when_idle: true,
        ..Default::default()
    };
    assert!(matches!(
        owner.submit_with_options(id, Some("idle".into()), "x".into(), idle_reject),
        Err(Error::Admission(AdmissionError::Idle))
    ));
    assert!(owner.conversation(id).unwrap().submissions.is_empty());

    let barrier = Arc::new(std::sync::Barrier::new(12));
    let threads: Vec<_> = (0..12)
        .map(|i| {
            let mut worker = owner.worker();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                worker.submit_with_options(
                    id,
                    Some(format!("key-{i}")),
                    "work".into(),
                    AdmissionOptions {
                        reject_when_busy: true,
                        ..Default::default()
                    },
                )
            })
        })
        .collect();
    let results: Vec<_> = threads.into_iter().map(|t| t.join().unwrap()).collect();
    assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|r| matches!(r, Err(Error::Admission(AdmissionError::Busy))))
            .count(),
        11
    );
    let winning_index = results.iter().position(Result::is_ok).unwrap();
    let winner = *results[winning_index].as_ref().unwrap();
    let both = AdmissionOptions {
        reject_when_busy: true,
        reject_when_idle: true,
        ..Default::default()
    };
    assert_eq!(
        owner
            .submit_with_options(
                id,
                Some(format!("key-{winning_index}")),
                "changed".into(),
                both
            )
            .unwrap(),
        winner
    );
    let queued = owner
        .submit_with_options(
            id,
            Some("idle".into()),
            "steer".into(),
            AdmissionOptions {
                mode: InputMode::Steer,
                ..idle_reject
            },
        )
        .unwrap();
    assert_ne!(winner, queued);
    drop(owner);
    let mut reopened = Harness::open_paused(&path, options()).unwrap();
    assert_eq!(reopened.conversation(id).unwrap().submissions.len(), 2);
    assert_eq!(
        reopened
            .submit_with_options(
                id,
                Some(format!("key-{winning_index}")),
                "again".into(),
                both
            )
            .unwrap(),
        winner
    );
    assert!(matches!(
        reopened.submit_with_options(id, None, "new".into(), both),
        Err(Error::Admission(AdmissionError::Busy))
    ));
    // Settling the active run makes idle rejection apply even with retained queued work.
    let (mut state, version) = reopened.load(id).unwrap();
    finish_failure(&mut state, "failure".into());
    reopened.save(&state, Expected::Version(version)).unwrap();
    assert_eq!(
        reopened
            .submit_with_options(id, Some("idle".into()), "retry".into(), both)
            .unwrap(),
        queued
    );
    assert!(matches!(
        reopened.submit_with_options(id, None, "new".into(), both),
        Err(Error::Admission(AdmissionError::Idle))
    ));
    assert_eq!(reopened.conversation(id).unwrap().submissions.len(), 2);
}

#[test]
fn concurrent_same_request_wins_before_busy_rejection() {
    let dir = tempfile::tempdir().unwrap();
    let mut owner = Harness::open_paused(dir.path().join("db"), options()).unwrap();
    let id = owner
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 10,
        })
        .unwrap();
    let barrier = Arc::new(std::sync::Barrier::new(10));
    let threads: Vec<_> = (0..10)
        .map(|_| {
            let mut worker = owner.worker();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                worker
                    .submit_with_options(
                        id,
                        Some("same".into()),
                        "input".into(),
                        AdmissionOptions {
                            reject_when_busy: true,
                            ..Default::default()
                        },
                    )
                    .unwrap()
            })
        })
        .collect();
    let ids: Vec<_> = threads.into_iter().map(|t| t.join().unwrap()).collect();
    assert!(ids.iter().all(|id| *id == ids[0]));
    assert_eq!(owner.conversation(id).unwrap().submissions.len(), 1);
}

#[test]
fn duplicate_input_is_returned_before_content_validation() {
    let dir = tempfile::tempdir().unwrap();
    let mut owner = Harness::open_paused(dir.path().join("db"), options()).unwrap();
    let id = owner
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 10,
        })
        .unwrap();
    let original = owner
        .submit(id, Some("request".into()), "hello".into())
        .unwrap();
    assert_eq!(
        owner
            .submit(id, Some("request".into()), " ".into())
            .unwrap(),
        original
    );
    assert!(owner.submit(id, Some("new".into()), " ".into()).is_err());
}

#[test]
fn concurrent_writes_deduplicate_and_merge_with_execution() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut owner = Harness::open_paused(&path, options()).unwrap();
    let id = owner
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 10,
        })
        .unwrap();
    owner.submit(id, None, "active".into()).unwrap();
    let mut worker = owner.worker();
    let (mut stale, version) = worker.load(id).unwrap();
    let barrier = Arc::new(std::sync::Barrier::new(8));
    let threads: Vec<_> = (0..8)
        .map(|i| {
            let mut writer = owner.worker();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                let shared = writer
                    .submit_write(id, Some("same".into()), "app.shared".into(), None)
                    .unwrap();
                let unique = writer
                    .submit_write(
                        id,
                        Some(format!("write-{i}")),
                        "app.unique".into(),
                        Some(serde_json::json!(i)),
                    )
                    .unwrap();
                (shared, unique)
            })
        })
        .collect();
    let results: Vec<_> = threads.into_iter().map(|t| t.join().unwrap()).collect();
    assert!(results.iter().all(|r| r.0 == results[0].0));
    stale.phase = Phase::Boundary {
        turns: 1,
        answer: Some("answer".into()),
    };
    worker.save(&stale, Expected::Version(version)).unwrap();
    drop(worker);
    drop(owner);
    let reopened = Harness::open_paused(&path, options()).unwrap();
    let state = reopened.conversation(id).unwrap();
    assert_eq!(state.application_writes.len(), 9);
    assert!(state.application_writes.iter().all(|w| w.placed));
    for (_, unique) in results {
        assert!(state.application_writes.iter().any(|w| w.id == unique));
    }
}

#[test]
fn input_and_write_race_reserves_request_id_across_types() {
    let dir = tempfile::tempdir().unwrap();
    let mut owner = Harness::open_paused(dir.path().join("db"), options()).unwrap();
    let id = owner
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 10,
        })
        .unwrap();
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let mut input = owner.worker();
    let mut write = owner.worker();
    let b = barrier.clone();
    let a = std::thread::spawn(move || {
        b.wait();
        input.submit_with_options(
            id,
            Some("shared".into()),
            "input".into(),
            AdmissionOptions {
                reject_when_busy: true,
                ..Default::default()
            },
        )
    });
    let b = std::thread::spawn(move || {
        barrier.wait();
        write.submit_write(id, Some("shared".into()), "app.test".into(), None)
    });
    let (a, b) = (a.join().unwrap(), b.join().unwrap());
    assert_eq!(usize::from(a.is_ok()) + usize::from(b.is_ok()), 1);
    assert!(matches!(a, Err(Error::Invalid(_))) || matches!(b, Err(Error::Invalid(_))));
    let state = owner.conversation(id).unwrap();
    assert_eq!(state.submissions.len() + state.application_writes.len(), 1);
}

#[test]
fn writes_racing_abort_survive_and_stale_execution_cannot_clear_abort() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut owner = Harness::open_paused(&path, options()).unwrap();
    let id = owner
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 10,
        })
        .unwrap();
    owner.submit(id, None, "active".into()).unwrap();
    let mut stale_worker = owner.worker();
    let (mut stale, version) = stale_worker.load(id).unwrap();
    let barrier = Arc::new(std::sync::Barrier::new(9));
    let mut threads = Vec::new();
    for i in 0..8 {
        let mut writer = owner.worker();
        let barrier = barrier.clone();
        threads.push(std::thread::spawn(move || {
            barrier.wait();
            writer
                .submit_write(id, Some(format!("write-{i}")), "app.test".into(), None)
                .unwrap()
        }));
    }
    let mut aborter = owner.worker();
    let b = barrier.clone();
    let abort = std::thread::spawn(move || {
        b.wait();
        aborter.request_abort(id).unwrap();
    });
    let ids: Vec<_> = threads.into_iter().map(|t| t.join().unwrap()).collect();
    abort.join().unwrap();
    stale.phase = Phase::Boundary {
        turns: 1,
        answer: Some("stale".into()),
    };
    assert!(
        stale_worker
            .save(&stale, Expected::Version(version))
            .is_err()
    );
    drop(stale_worker);
    drop(owner);
    let reopened = Harness::open_paused(&path, options()).unwrap();
    let state = reopened.conversation(id).unwrap();
    assert!(state.abort_requested);
    assert_eq!(state.application_writes.len(), 8);
    assert!(ids.iter().all(|id| {
        state
            .application_writes
            .iter()
            .any(|w| w.id == *id && w.placed)
    }));
}

#[tokio::test]
async fn conversation_abort_marks_owned_tree_atomically_and_recovers_child_first() {
    use crate::task::{Checkpoint, Task, TaskOwner, TaskState};
    use crate::task_scheduler::{Registry, Step};
    use crate::task_store::{Save, TaskStore};
    use serde_json::json;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("abort-tree.db");
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let config = Config {
        model: "test".into(),
        system_prompt: "".into(),
        max_output_tokens: 10,
    };
    let id = h.create_conversation(config.clone()).unwrap();
    let other = h.create_conversation(config).unwrap();
    let make = |id, owner| {
        Task::new(
            id,
            owner,
            Checkpoint {
                phase: "initial".into(),
                data: json!(null),
            },
            4,
        )
        .unwrap()
        .registered("tree".into(), true)
    };
    let root = make(Uuid::new_v4(), TaskOwner::Conversation(id));
    let child = make(Uuid::new_v4(), TaskOwner::Task(root.id()));
    let unrelated = make(Uuid::new_v4(), TaskOwner::Conversation(other));
    let ts = TaskStore::new(h.owner.store());
    ts.save(&[
        Save::Create(&root),
        Save::Create(&child),
        Save::Create(&unrelated),
    ])
    .unwrap();
    // A stale task version makes the entire composed transaction fail.
    let mut stale = h.conversation_tasks(id).unwrap();
    for record in &mut stale {
        record.task.request_abort().unwrap();
    }
    let snapshot = ts.load(root.id()).unwrap().unwrap();
    ts.update(&snapshot, |t| t.start()).unwrap();
    let (mut state, version) = h.load(id).unwrap();
    state.abort_requested = true;
    assert!(
        h.save_composed_locked(&state, Expected::Version(version), None, &[], &stale, None)
            .is_err()
    );
    assert!(!h.conversation(id).unwrap().abort_requested);
    assert!(
        !TaskStore::new(h.owner.store())
            .load(child.id())
            .unwrap()
            .unwrap()
            .task
            .abort_requested()
    );
    h.request_abort(id).unwrap();
    assert!(h.conversation(id).unwrap().abort_requested);
    let ts = TaskStore::new(h.owner.store());
    assert!(ts.load(root.id()).unwrap().unwrap().task.abort_requested());
    assert!(ts.load(child.id()).unwrap().unwrap().task.abort_requested());
    assert!(
        !ts.load(unrelated.id())
            .unwrap()
            .unwrap()
            .task
            .abort_requested()
    );
    drop(h);
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut registry = Registry::default();
    let log = calls.clone();
    registry.register_with_abort(
        "tree",
        true,
        |_: &Task| Ok(Step::Succeed(json!(null))),
        move |task: &Task| {
            log.lock().unwrap().push(task.id());
            Ok(Step::Succeed(json!(null)))
        },
    );
    let mut scheduler = h.attach_tasks(registry).unwrap();
    assert!(scheduler.tick(0).unwrap());
    assert!(scheduler.tick(0).unwrap());
    assert_eq!(*calls.lock().unwrap(), vec![child.id(), root.id()]);
    assert_eq!(
        scheduler.get(root.id()).unwrap().unwrap().state(),
        &TaskState::Cancelled
    );
    drop(scheduler);
    h.drive(id, &CancellationToken::new()).await.unwrap();
    assert!(!h.conversation(id).unwrap().abort_requested);
}

#[test]
fn abort_fences_root_and_stale_lifecycle_admissions() {
    use crate::task::{Checkpoint, Task, TaskOwner};
    use crate::task_scheduler::{Registry, Step};
    use serde_json::json;
    let dir = tempfile::tempdir().unwrap();
    let mut h = Harness::open_paused(dir.path().join("db"), options()).unwrap();
    let id = h
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 10,
        })
        .unwrap();
    let make = || {
        Task::new(
            Uuid::new_v4(),
            TaskOwner::Conversation(id),
            Checkpoint {
                phase: "initial".into(),
                data: json!(null),
            },
            4,
        )
        .unwrap()
        .registered("tree".into(), true)
    };
    let mut registry = Registry::default();
    registry.register("tree", true, |_: &Task| Ok(Step::Succeed(json!(null))));
    let scheduler = h.attach_tasks(registry).unwrap();
    scheduler.submit(&make()).unwrap();
    let mut worker = h.worker();
    let (stale, version) = worker.load(id).unwrap();
    h.request_abort(id).unwrap();
    assert!(scheduler.submit(&make()).is_err());
    let late = make();
    assert!(
        worker
            .save_composed(
                &stale,
                Expected::Version(version),
                None,
                std::slice::from_ref(&late)
            )
            .is_err()
    );
    assert!(scheduler.get(late.id()).unwrap().is_none());
}

#[test]
fn conversation_abort_fences_running_task_and_its_proposed_child() {
    use crate::task::{Checkpoint, Task, TaskOwner, TaskState, WaitFor};
    use crate::task_scheduler::{Registry, Step};
    use serde_json::json;
    use std::sync::mpsc;
    let dir = tempfile::tempdir().unwrap();
    let mut h = Harness::open_paused(dir.path().join("db"), options()).unwrap();
    let id = h
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 10,
        })
        .unwrap();
    let root = Task::new(
        Uuid::new_v4(),
        TaskOwner::Conversation(id),
        Checkpoint {
            phase: "initial".into(),
            data: json!(null),
        },
        4,
    )
    .unwrap()
    .registered("tree".into(), true);
    let child_id = Uuid::new_v4();
    let (started_tx, started_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let release_rx = std::sync::Mutex::new(release_rx);
    let mut registry = Registry::default();
    registry.register_with_abort(
        "tree",
        true,
        move |task: &Task| {
            started_tx.send(()).unwrap();
            release_rx.lock().unwrap().recv().unwrap();
            let child = Task::new(
                child_id,
                TaskOwner::Task(task.id()),
                task.checkpoint().clone(),
                4,
            )
            .unwrap()
            .registered("tree".into(), true);
            Ok(Step::Wait {
                checkpoint: task.checkpoint().clone(),
                condition: WaitFor::Tasks(vec![child_id]),
                children: vec![child],
            })
        },
        |_: &Task| Ok(Step::Succeed(json!(null))),
    );
    let driver = h.start_tasks(registry).unwrap();
    driver.submit(&root).unwrap();
    started_rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .unwrap();
    h.request_abort(id).unwrap();
    assert!(driver.get(root.id()).unwrap().unwrap().abort_requested());
    assert!(
        driver
            .submit(
                &Task::new(
                    Uuid::new_v4(),
                    TaskOwner::Conversation(id),
                    root.checkpoint().clone(),
                    4
                )
                .unwrap()
                .registered("tree".into(), true)
            )
            .is_err()
    );
    release_tx.send(()).unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        if driver.get(root.id()).unwrap().unwrap().state() == &TaskState::Cancelled {
            break;
        }
        assert!(std::time::Instant::now() < deadline);
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    assert!(driver.get(child_id).unwrap().is_none());
    driver.shutdown().unwrap();
}

#[test]
fn document_admissions_compete_retry_and_reopen() {
    use crate::document::{Address, Documents};
    use std::sync::atomic::{AtomicUsize, Ordering};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut owner = Harness::open_paused(&path, options()).unwrap();
    let id = owner
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 10,
        })
        .unwrap();
    let address = Address {
        scope: "session".into(),
        name: "counter".into(),
        key: "".into(),
    };
    let initial_snapshot = Documents::new(owner.owner.store()).snapshot().unwrap();
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let attempts = Arc::new(AtomicUsize::new(0));
    let threads: Vec<_> = (0..2)
        .map(|i| {
            let mut worker = owner.worker();
            let address = address.clone();
            let barrier = barrier.clone();
            let attempts = attempts.clone();
            std::thread::spawn(move || {
                let mut first = true;
                worker
                    .submit_with_documents(
                        id,
                        Some(format!("{i}")),
                        "input".into(),
                        AdmissionOptions::default(),
                        |docs| {
                            let n = docs
                                .get(&address)?
                                .map_or(0, |d| d.value["n"].as_u64().unwrap());
                            attempts.fetch_add(1, Ordering::SeqCst);
                            if first {
                                first = false;
                                barrier.wait();
                            }
                            docs.put(
                                &address,
                                serde_json::json!({"n": n + 1}).as_object().unwrap().clone(),
                            )
                        },
                    )
                    .unwrap()
            })
        })
        .collect();
    let ids: Vec<_> = threads.into_iter().map(|t| t.join().unwrap()).collect();
    assert_ne!(ids[0], ids[1]);
    assert!(attempts.load(Ordering::SeqCst) >= 3);
    let docs = Documents::new(owner.owner.store());
    let final_snapshot = docs.snapshot().unwrap();
    assert_eq!(final_snapshot, initial_snapshot + 2);
    assert!(docs.get_at(&address, initial_snapshot).unwrap().is_none());
    assert_eq!(
        docs.get_at(&address, initial_snapshot + 1)
            .unwrap()
            .unwrap()
            .value["n"],
        1
    );
    assert_eq!(
        Documents::new(owner.owner.store())
            .get(&address)
            .unwrap()
            .unwrap()
            .value["n"],
        2
    );
    drop(owner);
    let mut owner = Harness::open_paused(&path, options()).unwrap();
    assert_eq!(owner.conversation(id).unwrap().submissions.len(), 2);
    assert_eq!(
        Documents::new(owner.owner.store())
            .get(&address)
            .unwrap()
            .unwrap()
            .value["n"],
        2
    );
    let same = owner
        .submit_with_documents(
            id,
            Some("0".into()),
            "different".into(),
            AdmissionOptions {
                reject_when_busy: true,
                ..Default::default()
            },
            |_| panic!("duplicate callback"),
        )
        .unwrap();
    assert_eq!(same, ids[0]);
    let rejected = owner.submit_with_documents(
        id,
        Some("new".into()),
        "input".into(),
        AdmissionOptions {
            reject_when_busy: true,
            ..Default::default()
        },
        |_| panic!("rejected callback"),
    );
    assert!(matches!(
        rejected,
        Err(Error::Admission(AdmissionError::Busy))
    ));
    assert_eq!(
        Documents::new(owner.owner.store())
            .get(&address)
            .unwrap()
            .unwrap()
            .value["n"],
        2
    );
}

#[test]
fn document_admission_read_after_write_and_callback_failure_roll_back() {
    use crate::document::{Address, Documents};
    let dir = tempfile::tempdir().unwrap();
    let mut owner = Harness::open_paused(dir.path().join("db"), options()).unwrap();
    let id = owner
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 10,
        })
        .unwrap();
    let a = Address {
        scope: "s".into(),
        name: "a".into(),
        key: "".into(),
    };
    let b = Address {
        scope: "s".into(),
        name: "b".into(),
        key: "".into(),
    };
    let result = owner.submit_with_documents(
        id,
        None,
        "input".into(),
        AdmissionOptions::default(),
        |docs| {
            docs.get(&a)?;
            docs.put(&a, serde_json::Map::new())?;
            docs.get(&b)?;
            Ok(())
        },
    );
    assert!(matches!(
        result,
        Err(Error::Document(crate::document::Error::ReadAfterWrite))
    ));
    assert!(owner.conversation(id).unwrap().submissions.is_empty());
    assert!(
        Documents::new(owner.owner.store())
            .get(&a)
            .unwrap()
            .is_none()
    );
}

#[test]
fn document_admission_rechecks_read_only_dependencies() {
    use crate::document::{Address, Transaction};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut owner = Harness::open_paused(&path, options()).unwrap();
    let id = owner
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 10,
        })
        .unwrap();
    let dependency = Address {
        scope: "s".into(),
        name: "dependency".into(),
        key: "".into(),
    };
    let output = Address {
        scope: "s".into(),
        name: "output".into(),
        key: "".into(),
    };
    let external = durable_store::Store::open(&path).unwrap();
    let mut calls = 0;
    owner
        .submit_with_documents(
            id,
            Some("request".into()),
            "input".into(),
            AdmissionOptions::default(),
            |docs| {
                let old = docs.get(&dependency)?;
                docs.get(&output)?;
                calls += 1;
                if calls == 1 {
                    let mut tx = Transaction::new();
                    tx.put(
                        &dependency,
                        Expected::Missing,
                        serde_json::json!({"n": 7}).as_object().unwrap(),
                    )?;
                    tx.commit(&external)?;
                }
                let n = old.map_or(0, |d| d.value["n"].as_u64().unwrap());
                docs.put(
                    &output,
                    serde_json::json!({"n": n}).as_object().unwrap().clone(),
                )
            },
        )
        .unwrap();
    assert_eq!(calls, 2);
    assert_eq!(
        crate::document::Documents::new(&external)
            .get(&output)
            .unwrap()
            .unwrap()
            .value["n"],
        7
    );
}

#[test]
fn positioned_view_splits_a_single_immutable_batch() {
    let dir = tempfile::tempdir().unwrap();
    let mut h = Harness::open_paused(dir.path().join("db"), options()).unwrap();
    let id = h
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 10,
        })
        .unwrap();
    let (mut state, version) = h.load(id).unwrap();
    state.messages = (0..5)
        .map(|n| Message::User {
            content: n.to_string(),
        })
        .collect();
    h.save(&state, Expected::Version(version)).unwrap();
    let limit = std::num::NonZeroUsize::new(2).unwrap();
    let mut page = h.view_positioned(id, limit).unwrap();
    let mut entries = Vec::new();
    loop {
        entries.extend(page.entries);
        match page.older {
            Some(next) => page = h.view_positioned_page(id, next, limit).unwrap(),
            None => break,
        }
    }
    assert_eq!(entries.len(), 5);
    for (index, entry) in entries.iter().enumerate() {
        let suffix = h
            .view_positioned_page(id, entry.position.clone(), limit)
            .unwrap();
        assert_eq!(suffix.entries[0].message, state.messages[4 - index]);
        assert_eq!(suffix.entries[0].message, entry.message);
    }
    let mut bad = entries[0].position.clone();
    bad.remaining = Some(6);
    assert!(h.view_positioned_page(id, bad, limit).is_err());
}

#[test]
fn position_fork_uses_first_complete_snapshot_and_survives_reset_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let id = h
        .create_conversation(Config {
            model: "original".into(),
            system_prompt: "old".into(),
            max_output_tokens: 10,
        })
        .unwrap();
    let other = h
        .create_conversation(Config {
            model: "other".into(),
            system_prompt: "".into(),
            max_output_tokens: 10,
        })
        .unwrap();
    let (mut state, version) = h.load(id).unwrap();
    state.messages = (0..3)
        .map(|n| Message::User {
            content: n.to_string(),
        })
        .collect();
    h.save(&state, Expected::Version(version)).unwrap();
    let entries = h
        .view_positioned(id, std::num::NonZeroUsize::new(10).unwrap())
        .unwrap()
        .entries;
    let position = entries[0].position.clone();
    let interior = h.fork_at_position(id, entries[1].position.clone()).unwrap();
    assert_eq!(h.conversation(interior).unwrap().messages.len(), 2);
    assert!(h.fork_at_position(other, position.clone()).is_err());
    let (mut state, version) = h.load(id).unwrap();
    state.config.model = "later".into();
    h.save(&state, Expected::Version(version)).unwrap();
    h.reset(id, Some("replacement".into())).unwrap();
    drop(h);
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let child = h.fork_at_position(id, position).unwrap();
    let result = h.conversation(child).unwrap();
    assert_eq!(result.config.model, "original");
    assert_eq!(result.messages.len(), 3);
    assert!(result.resets.is_empty());
    drop(h);
    let h = Harness::open_paused(&path, options()).unwrap();
    assert_eq!(h.conversation(child).unwrap().messages, result.messages);
}

#[test]
fn position_fork_rejects_missing_or_mismatched_historical_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let mut h = Harness::open_paused(dir.path().join("db"), options()).unwrap();
    let id = h
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 10,
        })
        .unwrap();
    let (mut state, version) = h.load(id).unwrap();
    state.messages.push(Message::User {
        content: "one".into(),
    });
    h.save(&state, Expected::Version(version)).unwrap();
    let position = h
        .view_positioned(id, std::num::NonZeroUsize::MIN)
        .unwrap()
        .entries
        .remove(0)
        .position;
    let key = format!("runtime/fork/{id}/{:020}", 2).into_bytes();
    let original = h.owner.store().get(&key).unwrap().unwrap().value;
    let mut value: serde_json::Value = serde_json::from_slice(&original).unwrap();
    value["documents"]
        .as_object_mut()
        .unwrap()
        .remove("pi-rs/runtime/extensions");
    h.owner
        .store()
        .commit(
            &[],
            &[Mutation::Put {
                key: key.clone(),
                value: serde_json::to_vec(&value).unwrap(),
            }],
        )
        .unwrap();
    assert!(h.fork_at_position(id, position.clone()).is_err());
    let mut value: serde_json::Value = serde_json::from_slice(&original).unwrap();
    value["_transcript"] = serde_json::Value::Null;
    h.owner
        .store()
        .commit(
            &[],
            &[Mutation::Put {
                key: key.clone(),
                value: serde_json::to_vec(&value).unwrap(),
            }],
        )
        .unwrap();
    assert!(h.fork_at_position(id, position.clone()).is_err());
    h.owner
        .store()
        .commit(&[], &[Mutation::Delete { key }])
        .unwrap();
    assert!(h.fork_at_position(id, position).is_err());
}

#[test]
fn message_history_tracks_commit_state_across_batches_reset_and_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let id = h
        .create_conversation(Config {
            model: "first".into(),
            system_prompt: "".into(),
            max_output_tokens: 10,
        })
        .unwrap();
    let (mut state, version) = h.load(id).unwrap();
    state.messages.push(Message::User {
        content: "hello".into(),
    });
    h.save(&state, Expected::Version(version)).unwrap();
    let user = h
        .view_positioned(id, std::num::NonZeroUsize::MIN)
        .unwrap()
        .entries
        .remove(0);
    let user_history = user.history.unwrap();
    let (mut state, version) = h.load(id).unwrap();
    state.config.model = "second".into();
    state.phase = Phase::Boundary {
        turns: 1,
        answer: None,
    };
    state.tool_states.insert(
        "tool".into(),
        ExtensionState {
            version: 1,
            data: serde_json::json!({"step": 1}),
        },
    );
    state.messages.extend([
        Message::Assistant {
            content: "calling".into(),
            calls: vec![pi_rs_core::ToolCall {
                id: "call".into(),
                name: "tool".into(),
                arguments: "{}".into(),
            }],
            replay: None,
        },
        Message::Tool {
            call_id: "call".into(),
            content: "result".into(),
        },
    ]);
    h.save(&state, Expected::Version(version)).unwrap();
    let page = h
        .view_positioned(id, std::num::NonZeroUsize::new(10).unwrap())
        .unwrap();
    let tool = page.entries[0].history.as_ref().unwrap();
    let model = page.entries[1].history.as_ref().unwrap();
    assert_ne!(tool.id, model.id);
    assert_ne!(tool.id, user_history.id);
    assert_eq!(tool.commit, model.commit);
    assert!(tool.commit_boundary);
    assert!(!model.commit_boundary);
    assert_ne!(tool.commit, user_history.commit);
    let snapshot = |history: &MessageHistory| -> serde_json::Value {
        let key = format!("runtime/fork/{}/{:020}", history.origin, history.commit);
        serde_json::from_slice(&h.owner.store().get(key.as_bytes()).unwrap().unwrap().value)
            .unwrap()
    };
    assert_eq!(snapshot(&user_history)["config"]["model"], "first");
    let historical = snapshot(tool);
    assert_eq!(historical["config"]["model"], "second");
    assert_eq!(historical["tool_states"]["tool"]["data"]["step"], 1);
    assert!(historical["phase"].get("Boundary").is_some());
    let saved = serde_json::to_value(tool).unwrap();
    let position = page.entries[0].position.clone();
    let (mut state, version) = h.load(id).unwrap();
    state.phase = Phase::Idle;
    h.save(&state, Expected::Version(version)).unwrap();
    h.reset(id, Some("replacement".into())).unwrap();
    drop(h);
    let h = Harness::open_paused(&path, options()).unwrap();
    let old = h
        .view_positioned_page(id, position, std::num::NonZeroUsize::MIN)
        .unwrap();
    assert_eq!(
        serde_json::to_value(old.entries[0].history.as_ref().unwrap()).unwrap(),
        saved
    );
}

#[test]
fn legacy_batches_are_readable_but_cannot_claim_entry_history_or_fork() {
    let dir = tempfile::tempdir().unwrap();
    let mut h = Harness::open_paused(dir.path().join("db"), options()).unwrap();
    let id = h
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 10,
        })
        .unwrap();
    let (mut state, _) = h.load(id).unwrap();
    let message = Message::User {
        content: "legacy".into(),
    };
    let transcript = crate::transcript::Transcript::new(h.owner.store(), id);
    let (batch, cursor) = transcript
        .prepare_write(
            &serde_json::to_value(TranscriptChange::Replace(vec![message])).unwrap(),
            false,
        )
        .unwrap();
    state.transcript = Some(cursor);
    state.messages.clear();
    let mut mutations = batch.mutations;
    mutations.push(Mutation::Put {
        key: Harness::key(id),
        value: serde_json::to_vec(&state).unwrap(),
    });
    h.owner.store().commit(&batch.checks, &mutations).unwrap();
    let old = h
        .view_positioned(id, std::num::NonZeroUsize::MIN)
        .unwrap()
        .entries
        .remove(0);
    assert!(old.history.is_none());
    assert!(h.fork_at_position(id, old.position).is_err());
    let (mut state, version) = h.load(id).unwrap();
    state.messages.push(Message::User {
        content: "new".into(),
    });
    h.save(&state, Expected::Version(version)).unwrap();
    let page = h
        .view_positioned(id, std::num::NonZeroUsize::new(10).unwrap())
        .unwrap();
    assert!(page.entries[0].history.is_some());
    assert!(page.entries[1].history.is_none());
}

#[test]
fn malformed_entry_metadata_fails_closed() {
    let id = Uuid::new_v4();
    let message = Message::User {
        content: "x".into(),
    };
    for ids in [vec![], vec![Uuid::nil()], vec![id, id]] {
        let messages = if ids.len() == 2 {
            vec![message.clone(), message.clone()]
        } else {
            vec![message.clone()]
        };
        assert!(
            TranscriptChange::AppendV2 {
                messages,
                ids,
                commit: 1,
                origin: Uuid::new_v4()
            }
            .parts()
            .is_err()
        );
    }
    assert!(
        TranscriptChange::AppendV2 {
            messages: vec![message],
            ids: vec![id],
            commit: 0,
            origin: Uuid::new_v4()
        }
        .parts()
        .is_err()
    );
}

#[test]
fn admission_tasks_are_atomic_idempotent_and_recoverable() {
    use crate::document::{Address, Documents};
    use crate::task::{Checkpoint, Task, TaskOwner};
    use crate::task_scheduler::{Registry, Step};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let id = h
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 10,
        })
        .unwrap();
    let addr = Address {
        scope: "s".into(),
        name: "admission".into(),
        key: "".into(),
    };
    let make = |task_id, kind: &str| {
        Task::new(
            task_id,
            TaskOwner::Conversation(id),
            Checkpoint {
                phase: "initial".into(),
                data: serde_json::json!(null),
            },
            4,
        )
        .unwrap()
        .registered(kind.into(), true)
    };
    let mut registry = Registry::default();
    registry.register("job", true, |_: &Task| {
        Ok(Step::Succeed(serde_json::json!(null)))
    });
    let driver = h.start_tasks(registry).unwrap();
    let bad_id = Uuid::new_v4();
    let invalid = h.submit_with_documents(
        id,
        Some("bad".into()),
        "hello".into(),
        AdmissionOptions::default(),
        |tx| {
            tx.get(&addr)?;
            tx.put(&addr, serde_json::Map::new())?;
            tx.create_task(make(bad_id, "unknown"))
        },
    );
    assert!(invalid.is_err());
    assert!(h.conversation(id).unwrap().submissions.is_empty());
    assert!(
        Documents::new(h.owner.store())
            .get(&addr)
            .unwrap()
            .is_none()
    );
    assert!(driver.get(bad_id).unwrap().is_none());
    let other = h
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 10,
        })
        .unwrap();
    for task in [
        make(Uuid::new_v4(), "job").registered("job".into(), false),
        make(Uuid::new_v4(), "job")
            .registered_versioned("job".into(), true, 2, serde_json::Value::Null)
            .unwrap(),
        Task::new(
            Uuid::new_v4(),
            TaskOwner::Conversation(other),
            Checkpoint {
                phase: "initial".into(),
                data: serde_json::json!(null),
            },
            4,
        )
        .unwrap()
        .registered("job".into(), true),
    ] {
        assert!(
            h.submit_with_documents(
                id,
                None,
                "input".into(),
                AdmissionOptions::default(),
                |tx| tx.create_task(task.clone())
            )
            .is_err()
        );
    }
    assert!(h.conversation(id).unwrap().submissions.is_empty());
    let task_id = Uuid::new_v4();
    let sid = h
        .submit_with_documents(
            id,
            Some("ok".into()),
            "hello".into(),
            AdmissionOptions::default(),
            |tx| {
                tx.get(&addr)?;
                tx.put(
                    &addr,
                    serde_json::json!({"value": 1}).as_object().unwrap().clone(),
                )?;
                tx.create_task(make(task_id, "job"))
            },
        )
        .unwrap();
    assert_eq!(
        h.submit_with_documents(
            id,
            Some("ok".into()),
            "".into(),
            AdmissionOptions::default(),
            |_| panic!("duplicate callback")
        )
        .unwrap(),
        sid
    );
    driver.shutdown().unwrap();
    drop(h);
    let h = Harness::open_paused(&path, options()).unwrap();
    assert_eq!(h.conversation(id).unwrap().submissions.len(), 1);
    assert_eq!(
        Documents::new(h.owner.store())
            .get(&addr)
            .unwrap()
            .unwrap()
            .value["value"],
        1
    );
    let mut registry = Registry::default();
    registry.register("job", true, |_: &Task| {
        Ok(Step::Succeed(serde_json::json!(null)))
    });
    let mut scheduler = h.attach_tasks(registry).unwrap();
    for _ in 0..5 {
        if scheduler.get(task_id).unwrap().unwrap().is_terminal() {
            break;
        }
        scheduler.tick(0).unwrap();
    }
    assert!(matches!(
        scheduler.get(task_id).unwrap().unwrap().state(),
        crate::task::TaskState::Succeeded(_)
    ));
}

#[test]
fn admission_task_racing_conversation_abort_rolls_back() {
    use crate::document::{Address, Documents};
    use crate::task::{Checkpoint, Task, TaskOwner};
    use crate::task_scheduler::{Registry, Step};
    let dir = tempfile::tempdir().unwrap();
    let mut h = Harness::open_paused(dir.path().join("db"), options()).unwrap();
    let id = h
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 10,
        })
        .unwrap();
    // Ensure abort leaves a durable abort mark.
    h.submit(id, None, "active".into()).unwrap();
    let mut registry = Registry::default();
    registry.register("job", true, |_: &Task| {
        Ok(Step::Succeed(serde_json::json!(null)))
    });
    let driver = h.start_tasks(registry).unwrap();
    let addr = Address {
        scope: "s".into(),
        name: "race".into(),
        key: "".into(),
    };
    let task_id = Uuid::new_v4();
    let (ready_tx, ready_rx) = std::sync::mpsc::channel();
    let (go_tx, go_rx) = std::sync::mpsc::channel();
    let mut worker = h.worker();
    let address = addr.clone();
    let thread = std::thread::spawn(move || {
        let mut first = true;
        worker.submit_with_documents(
            id,
            Some("race".into()),
            "input".into(),
            AdmissionOptions::default(),
            |tx| {
                tx.get(&address)?;
                tx.put(&address, serde_json::Map::new())?;
                tx.create_task(
                    Task::new(
                        task_id,
                        TaskOwner::Conversation(id),
                        Checkpoint {
                            phase: "initial".into(),
                            data: serde_json::json!(null),
                        },
                        4,
                    )
                    .unwrap()
                    .registered("job".into(), true),
                )?;
                if first {
                    first = false;
                    ready_tx.send(()).unwrap();
                    go_rx.recv().unwrap();
                }
                Ok(())
            },
        )
    });
    ready_rx.recv().unwrap();
    h.request_abort(id).unwrap();
    go_tx.send(()).unwrap();
    assert!(thread.join().unwrap().is_err());
    assert_eq!(h.conversation(id).unwrap().submissions.len(), 1);
    assert!(
        Documents::new(h.owner.store())
            .get(&addr)
            .unwrap()
            .is_none()
    );
    assert!(driver.get(task_id).unwrap().is_none());
    driver.shutdown().unwrap();
}

#[test]
fn entry_id_fork_uses_exact_snapshot_across_reset_and_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let id = h
        .create_conversation(Config {
            model: "old".into(),
            system_prompt: "prompt".into(),
            max_output_tokens: 10,
        })
        .unwrap();
    let other = h
        .create_conversation(Config {
            model: "other".into(),
            system_prompt: "".into(),
            max_output_tokens: 10,
        })
        .unwrap();
    // Cross an observation page boundary before the message is committed.
    for _ in 0..130 {
        let (state, version) = h.load(id).unwrap();
        h.save(&state, Expected::Version(version)).unwrap();
    }
    let (mut state, version) = h.load(id).unwrap();
    state.provider_adapter = Some("adapter-old".into());
    state.allowed_tools = Some(vec!["tool-old".into()]);
    state.messages = vec![
        Message::User {
            content: "same".into(),
        },
        Message::User {
            content: "same".into(),
        },
    ];
    state.aborted_generations.push(AbortedGeneration {
        request_id: Uuid::new_v4(),
        text: "partial".into(),
    });
    h.save(&state, Expected::Version(version)).unwrap();
    let page = h
        .view_positioned(id, std::num::NonZeroUsize::new(10).unwrap())
        .unwrap();
    let boundary = page.entries[0].history.as_ref().unwrap().id;
    let interior = page.entries[1].history.as_ref().unwrap().id;
    assert_ne!(boundary, interior);
    assert!(h.fork_by_entry_id(id, interior).is_err());
    assert!(h.fork_by_entry_id(other, boundary).is_err());
    assert!(h.fork_by_entry_id(id, Uuid::new_v4()).is_err());
    let (mut state, version) = h.load(id).unwrap();
    state.config.model = "new".into();
    state.provider_adapter = None;
    state.allowed_tools = None;
    h.save(&state, Expected::Version(version)).unwrap();
    h.reset(id, Some("replacement".into())).unwrap();
    drop(h);
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let (mut state, version) = h.load(id).unwrap();
    state.abort_requested = true;
    h.save(&state, Expected::Version(version)).unwrap();
    let before = h.conversation(id).unwrap();
    let child = h.fork_by_entry_id(id, boundary).unwrap();
    let result = h.conversation(child).unwrap();
    assert_eq!(result.config.model, "old");
    assert_eq!(result.provider_adapter.as_deref(), Some("adapter-old"));
    assert_eq!(result.allowed_tools, Some(vec!["tool-old".into()]));
    assert_eq!(result.messages.len(), 2);
    assert!(result.aborted_generations.is_empty());
    let after = h.conversation(id).unwrap();
    assert!(after.abort_requested);
    assert!(!result.abort_requested);
    assert_eq!(
        serde_json::to_value(before).unwrap(),
        serde_json::to_value(after).unwrap()
    );
    let descendant = h.fork_by_entry_id(child, boundary).unwrap();
    assert_eq!(
        h.conversation(descendant).unwrap().messages,
        result.messages
    );
    drop(h);
    let h = Harness::open_paused(&path, options()).unwrap();
    assert_eq!(h.conversation(child).unwrap().messages, result.messages);
}

#[test]
fn entry_id_fork_rejects_incomplete_snapshot_settings() {
    let dir = tempfile::tempdir().unwrap();
    let mut h = Harness::open_paused(dir.path().join("db"), options()).unwrap();
    let id = h
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 10,
        })
        .unwrap();
    let (mut state, version) = h.load(id).unwrap();
    state.messages.push(Message::User {
        content: "one".into(),
    });
    h.save(&state, Expected::Version(version)).unwrap();
    let entry = h
        .view_positioned(id, std::num::NonZeroUsize::MIN)
        .unwrap()
        .entries
        .remove(0)
        .history
        .unwrap();
    let key = format!("runtime/fork/{id}/{:020}", entry.commit).into_bytes();
    let original = h.owner.store().get(&key).unwrap().unwrap().value;
    for name in [
        "pi-rs/runtime/provider-adapter",
        "pi-rs/runtime/allowed-tools",
    ] {
        let mut value: serde_json::Value = serde_json::from_slice(&original).unwrap();
        value["documents"].as_object_mut().unwrap().remove(name);
        h.owner
            .store()
            .commit(
                &[],
                &[Mutation::Put {
                    key: key.clone(),
                    value: serde_json::to_vec(&value).unwrap(),
                }],
            )
            .unwrap();
        assert!(h.fork_by_entry_id(id, entry.id).is_err());
    }
}

#[test]
fn interior_entry_fork_requires_proven_stable_state_and_valid_context() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let id = h
        .create_conversation(Config {
            model: "historical".into(),
            system_prompt: "prompt".into(),
            max_output_tokens: 10,
        })
        .unwrap();
    let (mut state, version) = h.load(id).unwrap();
    state.provider_adapter = Some("adapter".into());
    h.save(&state, Expected::Version(version)).unwrap();
    let (mut state, version) = h.load(id).unwrap();
    state.messages.extend([
        Message::User {
            content: "first".into(),
        },
        Message::User {
            content: "second".into(),
        },
    ]);
    h.save(&state, Expected::Version(version)).unwrap();
    let entries = h
        .view_positioned(id, std::num::NonZeroUsize::new(10).unwrap())
        .unwrap()
        .entries;
    let first = entries[1].history.as_ref().unwrap().id;
    let (mut state, version) = h.load(id).unwrap();
    state.config.model = "new".into();
    h.save(&state, Expected::Version(version)).unwrap();
    drop(h);
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let child = h.fork_by_entry_id(id, first).unwrap();
    let fork = h.conversation(child).unwrap();
    assert_eq!(fork.config.model, "historical");
    assert_eq!(fork.provider_adapter.as_deref(), Some("adapter"));
    assert_eq!(
        fork.messages,
        vec![Message::User {
            content: "first".into()
        }]
    );
    let (mut state, version) = h.load(id).unwrap();
    state.messages.extend([
        Message::Assistant {
            content: "".into(),
            calls: vec![pi_rs_core::ToolCall {
                id: "call".into(),
                name: "tool".into(),
                arguments: "{}".into(),
            }],
            replay: None,
        },
        Message::Tool {
            call_id: "call".into(),
            content: "result".into(),
        },
        Message::User {
            content: "after".into(),
        },
    ]);
    h.save(&state, Expected::Version(version)).unwrap();
    let entries = h
        .view_positioned(id, std::num::NonZeroUsize::new(10).unwrap())
        .unwrap()
        .entries;
    let call = entries[2].history.as_ref().unwrap().id;
    let result = entries[1].history.as_ref().unwrap().id;
    let before = h.conversation(id).unwrap();
    assert!(h.fork_by_entry_id(id, call).is_err());
    assert_eq!(
        serde_json::to_value(&before).unwrap(),
        serde_json::to_value(h.conversation(id).unwrap()).unwrap()
    );
    let child = h.fork_by_entry_id(id, result).unwrap();
    assert_eq!(
        h.conversation(child).unwrap().messages,
        before.messages[..4]
    );
    drop(h);
    let h = Harness::open_paused(&path, options()).unwrap();
    assert_eq!(h.conversation(child).unwrap().messages.len(), 4);
}

#[test]
fn current_fork_retries_when_parent_changes_before_document_snapshot() {
    use crate::document::Address;
    let dir = tempfile::tempdir().unwrap();
    let mut h = Harness::open_paused(dir.path().join("db"), options()).unwrap();
    let parent = h
        .create_conversation(Config {
            model: "old".into(),
            system_prompt: "".into(),
            max_output_tokens: 10,
        })
        .unwrap();
    let address = Address {
        scope: parent.to_string(),
        name: "note".into(),
        key: "x".into(),
    };
    let mut doc_key = b"durable/document/".to_vec();
    doc_key.extend(serde_json::to_vec(&[&address.scope, &address.name, &address.key]).unwrap());
    h.owner
        .store()
        .commit(
            &[],
            &[Mutation::Put {
                key: doc_key.clone(),
                value: br#"{"n":1}"#.to_vec(),
            }],
        )
        .unwrap();
    let mut writer = h.worker();
    let mut done = false;
    let child = h
        .fork_with_load_hook(parent, || {
            if done {
                return;
            }
            done = true;
            let (mut state, version) = writer.load(parent).unwrap();
            state.config.model = "new".into();
            let mut prepared = writer
                .prepare_saves(&[(&state, Expected::Version(version))])
                .unwrap();
            prepared.mutations.push(Mutation::Put {
                key: doc_key.clone(),
                value: br#"{"n":2}"#.to_vec(),
            });
            writer.commit_conversation_save(prepared).unwrap();
        })
        .unwrap();
    assert_eq!(h.conversation(child).unwrap().config.model, "new");
    let copied = Address {
        scope: child.to_string(),
        ..address
    };
    assert_eq!(h.document(&copied).unwrap().unwrap().value["n"], 2);
}

#[test]
fn interior_fork_captures_same_commit_config_and_is_independent_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let id = h
        .create_conversation(Config {
            model: "before".into(),
            system_prompt: "before".into(),
            max_output_tokens: 10,
        })
        .unwrap();
    let (mut state, version) = h.load(id).unwrap();
    state.config = Config {
        model: "at-entry".into(),
        system_prompt: "historical".into(),
        max_output_tokens: 42,
    };
    state.messages.extend([
        Message::User {
            content: "included".into(),
        },
        Message::User {
            content: "excluded".into(),
        },
    ]);
    h.save(&state, Expected::Version(version)).unwrap();
    let entries = h
        .view_positioned(id, std::num::NonZeroUsize::new(10).unwrap())
        .unwrap()
        .entries;
    let entry = entries[1].history.as_ref().unwrap().id;
    let position = entries[1].position.clone();
    let (mut state, version) = h.load(id).unwrap();
    state.config.model = "later".into();
    h.save(&state, Expected::Version(version)).unwrap();
    drop(h);
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let child = h.fork_by_entry_id(id, entry).unwrap();
    let by_position = h.fork_at_position(id, position).unwrap();
    for fork in [child, by_position] {
        let result = h.conversation(fork).unwrap();
        assert_eq!(result.config.model, "at-entry");
        assert_eq!(result.config.system_prompt, "historical");
        assert_eq!(result.config.max_output_tokens, 42);
        assert_eq!(
            result.messages,
            vec![Message::User {
                content: "included".into()
            }]
        );
    }
    let (mut state, version) = h.load(child).unwrap();
    state.config.model = "child".into();
    state.messages.push(Message::User {
        content: "child-only".into(),
    });
    h.save(&state, Expected::Version(version)).unwrap();
    drop(h);
    let h = Harness::open_paused(&path, options()).unwrap();
    assert_eq!(h.conversation(id).unwrap().config.model, "later");
    assert_eq!(h.conversation(id).unwrap().messages.len(), 2);
    assert_eq!(h.conversation(child).unwrap().config.model, "child");
    assert_eq!(h.conversation(child).unwrap().messages.len(), 2);
    assert_eq!(
        h.conversation(by_position).unwrap().config.model,
        "at-entry"
    );
    assert_eq!(h.conversation(by_position).unwrap().messages.len(), 1);
}

#[test]
fn config_change_does_not_authorize_interior_tool_batch_cut() {
    let dir = tempfile::tempdir().unwrap();
    let mut h = Harness::open_paused(dir.path().join("db"), options()).unwrap();
    let id = h
        .create_conversation(Config {
            model: "before".into(),
            system_prompt: "".into(),
            max_output_tokens: 10,
        })
        .unwrap();
    let (mut state, version) = h.load(id).unwrap();
    state.config.model = "after".into();
    state.messages.extend([
        Message::User {
            content: "before tool".into(),
        },
        Message::Assistant {
            content: "".into(),
            calls: vec![pi_rs_core::ToolCall {
                id: "call".into(),
                name: "tool".into(),
                arguments: "{}".into(),
            }],
            replay: None,
        },
        Message::Tool {
            call_id: "call".into(),
            content: "result".into(),
        },
    ]);
    h.save(&state, Expected::Version(version)).unwrap();
    let entries = h
        .view_positioned(id, std::num::NonZeroUsize::new(10).unwrap())
        .unwrap()
        .entries;
    assert!(
        h.fork_by_entry_id(id, entries[2].history.as_ref().unwrap().id)
            .is_err()
    );
    assert!(h.fork_at_position(id, entries[1].position.clone()).is_err());
    assert!(
        h.fork_by_entry_id(id, entries[0].history.as_ref().unwrap().id)
            .is_ok()
    );
}

#[test]
fn inherited_reset_boundary_forks_from_its_origin_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let parent = h
        .create_conversation(Config {
            model: "at-reset".into(),
            system_prompt: "".into(),
            max_output_tokens: 10,
        })
        .unwrap();
    let (mut state, version) = h.load(parent).unwrap();
    state.messages.push(Message::User {
        content: "before".into(),
    });
    h.save(&state, Expected::Version(version)).unwrap();
    let old = h
        .view_positioned(parent, std::num::NonZeroUsize::MIN)
        .unwrap()
        .entries[0]
        .history
        .as_ref()
        .unwrap()
        .id;
    h.reset(parent, Some("handoff".into())).unwrap();
    let entry = h
        .view_positioned(parent, std::num::NonZeroUsize::MIN)
        .unwrap()
        .entries[0]
        .history
        .as_ref()
        .unwrap()
        .id;
    let child = h.fork(parent).unwrap();
    let (mut state, version) = h.load(parent).unwrap();
    state.config.model = "later".into();
    h.save(&state, Expected::Version(version)).unwrap();
    drop(h);
    let mut h = Harness::open_paused(&path, options()).unwrap();
    assert!(h.fork_by_entry_id(child, old).is_err());
    let fork = h.fork_by_entry_id(child, entry).unwrap();
    let result = h.conversation(fork).unwrap();
    assert_eq!(result.config.model, "at-reset");
    assert_eq!(result.messages, h.conversation(child).unwrap().messages);
    assert_eq!(result.resets.len(), 1);
}

#[test]
fn inherited_entry_forks_use_visible_origin_commit_and_documents() {
    use crate::document::Address;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let parent = h
        .create_conversation(Config {
            model: "origin".into(),
            system_prompt: "".into(),
            max_output_tokens: 10,
        })
        .unwrap();
    let address = Address {
        scope: parent.to_string(),
        name: "note".into(),
        key: "x".into(),
    };
    let mut doc_key = b"durable/document/".to_vec();
    doc_key.extend(serde_json::to_vec(&[&address.scope, &address.name, &address.key]).unwrap());
    let (mut state, version) = h.load(parent).unwrap();
    state.messages.push(Message::User {
        content: "same".into(),
    });
    let mut prepared = h
        .prepare_saves(&[(&state, Expected::Version(version))])
        .unwrap();
    prepared.mutations.push(Mutation::Put {
        key: doc_key.clone(),
        value: br#"{"n":1}"#.to_vec(),
    });
    h.commit_conversation_save(prepared).unwrap();
    let first = h
        .view_positioned(parent, std::num::NonZeroUsize::MIN)
        .unwrap()
        .entries[0]
        .history
        .as_ref()
        .unwrap()
        .id;
    let (mut state, version) = h.load(parent).unwrap();
    state.messages.push(Message::User {
        content: "same".into(),
    });
    h.save(&state, Expected::Version(version)).unwrap();
    let second = h
        .view_positioned(parent, std::num::NonZeroUsize::MIN)
        .unwrap()
        .entries[0]
        .history
        .as_ref()
        .unwrap()
        .id;
    let child = h.fork(parent).unwrap();
    let grandchild = h.fork(child).unwrap();
    let (mut state, version) = h.load(parent).unwrap();
    state.config.model = "later".into();
    let mut prepared = h
        .prepare_saves(&[(&state, Expected::Version(version))])
        .unwrap();
    prepared.mutations.push(Mutation::Put {
        key: doc_key,
        value: br#"{"n":2}"#.to_vec(),
    });
    h.commit_conversation_save(prepared).unwrap();
    drop(h);
    let mut h = Harness::open_paused(&path, options()).unwrap();
    for (id, expected_len) in [(first, 1), (second, 2)] {
        let fork = h.fork_by_entry_id(grandchild, id).unwrap();
        let result = h.conversation(fork).unwrap();
        assert_eq!(result.messages.len(), expected_len);
        assert_eq!(result.config.model, "origin");
        let copied = Address {
            scope: fork.to_string(),
            ..address.clone()
        };
        assert_eq!(h.document(&copied).unwrap().unwrap().value["n"], 1);
    }
    h.reset(grandchild, Some("replacement".into())).unwrap();
    assert!(h.fork_by_entry_id(grandchild, first).is_err());
    assert!(h.fork_by_entry_id(grandchild, second).is_err());
    drop(h);
    let mut h = Harness::open_paused(&path, options()).unwrap();
    assert!(h.fork_by_entry_id(grandchild, first).is_err());
}

#[test]
fn inherited_entry_rejects_materialized_interior_fork() {
    let dir = tempfile::tempdir().unwrap();
    let mut h = Harness::open_paused(dir.path().join("db"), options()).unwrap();
    let parent = h
        .create_conversation(Config {
            model: "x".into(),
            system_prompt: "".into(),
            max_output_tokens: 10,
        })
        .unwrap();
    let (mut state, version) = h.load(parent).unwrap();
    state.messages.extend((0..3).map(|_| Message::User {
        content: "same".into(),
    }));
    h.save(&state, Expected::Version(version)).unwrap();
    let entries = h
        .view_positioned(parent, std::num::NonZeroUsize::new(10).unwrap())
        .unwrap()
        .entries;
    let original_id = entries[2].history.as_ref().unwrap().id;
    let child = h
        .fork_at_position(parent, entries[1].position.clone())
        .unwrap();
    assert!(h.fork_by_entry_id(child, original_id).is_err());
}

#[test]
fn inherited_entry_rejects_ambiguous_and_unannotated_visible_batches() {
    let dir = tempfile::tempdir().unwrap();
    let mut h = Harness::open_paused(dir.path().join("db"), options()).unwrap();
    let parent = h
        .create_conversation(Config {
            model: "x".into(),
            system_prompt: "".into(),
            max_output_tokens: 10,
        })
        .unwrap();
    let (mut state, version) = h.load(parent).unwrap();
    state.messages.push(Message::User {
        content: "first".into(),
    });
    h.save(&state, Expected::Version(version)).unwrap();
    let id = h
        .view_positioned(parent, std::num::NonZeroUsize::MIN)
        .unwrap()
        .entries[0]
        .history
        .as_ref()
        .unwrap()
        .id;
    let (mut state, version) = h.load(parent).unwrap();
    state.messages.push(Message::User {
        content: "second".into(),
    });
    h.save(&state, Expected::Version(version)).unwrap();
    let child = h.fork(parent).unwrap();
    let cursor = serde_json::to_value(h.conversation(child).unwrap().transcript.unwrap()).unwrap();
    let link: Uuid = serde_json::from_value(cursor["next"]["id"].clone()).unwrap();
    let mut key = b"durable/transcript/entry/".to_vec();
    key.extend_from_slice(link.as_bytes());
    let original = h.owner.store().get(&key).unwrap().unwrap().value;
    let mut entry: serde_json::Value = serde_json::from_slice(&original).unwrap();
    let batch = entry["value"]
        .as_object_mut()
        .unwrap()
        .values_mut()
        .next()
        .unwrap();
    batch["ids"][0] = serde_json::to_value(id).unwrap();
    h.owner
        .store()
        .commit(
            &[],
            &[Mutation::Put {
                key: key.clone(),
                value: serde_json::to_vec(&entry).unwrap(),
            }],
        )
        .unwrap();
    assert!(h.fork_by_entry_id(child, id).is_err());
    let mut entry: serde_json::Value = serde_json::from_slice(&original).unwrap();
    let messages = entry["value"].as_object().unwrap().values().next().unwrap()["messages"].clone();
    entry["value"] = serde_json::json!({"Append": messages});
    h.owner
        .store()
        .commit(
            &[],
            &[Mutation::Put {
                key,
                value: serde_json::to_vec(&entry).unwrap(),
            }],
        )
        .unwrap();
    assert!(h.fork_by_entry_id(child, id).is_err());
}

#[test]
fn entry_id_fork_checks_inherited_chain_before_selecting_local_match() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let parent = h
        .create_conversation(Config {
            model: "origin".into(),
            system_prompt: "".into(),
            max_output_tokens: 10,
        })
        .unwrap();
    let (mut state, version) = h.load(parent).unwrap();
    state.messages.push(Message::User {
        content: "inherited".into(),
    });
    h.save(&state, Expected::Version(version)).unwrap();
    let inherited = h
        .view_positioned(parent, std::num::NonZeroUsize::MIN)
        .unwrap()
        .entries[0]
        .history
        .as_ref()
        .unwrap()
        .id;
    let child = h.fork(parent).unwrap();
    let (mut state, version) = h.load(child).unwrap();
    state.messages.push(Message::User {
        content: "local".into(),
    });
    h.save(&state, Expected::Version(version)).unwrap();
    let local = h
        .view_positioned(child, std::num::NonZeroUsize::MIN)
        .unwrap()
        .entries[0]
        .history
        .as_ref()
        .unwrap()
        .id;
    drop(h);
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let local_fork = h.fork_by_entry_id(child, local).unwrap();
    assert_eq!(h.conversation(local_fork).unwrap().messages.len(), 2);
    let inherited_fork = h.fork_by_entry_id(child, inherited).unwrap();
    assert_eq!(h.conversation(inherited_fork).unwrap().messages.len(), 1);

    let cursor = h.conversation(child).unwrap().transcript.unwrap();
    let transcript = crate::transcript::Transcript::new(h.owner.store(), child);
    let page = transcript
        .read(&cursor, std::num::NonZeroUsize::MIN)
        .unwrap();
    let inherited_cursor = serde_json::to_value(page.next).unwrap();
    let link: Uuid = serde_json::from_value(inherited_cursor["next"]["id"].clone()).unwrap();
    let mut key = b"durable/transcript/entry/".to_vec();
    key.extend_from_slice(link.as_bytes());
    let original = h.owner.store().get(&key).unwrap().unwrap().value;
    let before = serde_json::to_value(h.conversation(child).unwrap()).unwrap();
    let mut entry: serde_json::Value = serde_json::from_slice(&original).unwrap();
    entry["value"]
        .as_object_mut()
        .unwrap()
        .values_mut()
        .next()
        .unwrap()["ids"][0] = serde_json::to_value(local).unwrap();
    h.owner
        .store()
        .commit(
            &[],
            &[Mutation::Put {
                key: key.clone(),
                value: serde_json::to_vec(&entry).unwrap(),
            }],
        )
        .unwrap();
    assert!(h.fork_by_entry_id(child, local).is_err());
    assert_eq!(
        serde_json::to_value(h.conversation(child).unwrap()).unwrap(),
        before
    );
    let mut entry: serde_json::Value = serde_json::from_slice(&original).unwrap();
    let messages = entry["value"].as_object().unwrap().values().next().unwrap()["messages"].clone();
    entry["value"] = serde_json::json!({"Append": messages});
    h.owner
        .store()
        .commit(
            &[],
            &[Mutation::Put {
                key: key.clone(),
                value: serde_json::to_vec(&entry).unwrap(),
            }],
        )
        .unwrap();
    assert!(h.fork_by_entry_id(child, local).is_err());
    h.owner
        .store()
        .commit(
            &[],
            &[Mutation::Put {
                key,
                value: original,
            }],
        )
        .unwrap();
    h.reset(child, Some("replacement".into())).unwrap();
    drop(h);
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let fork = h.fork_by_entry_id(child, local).unwrap();
    assert_eq!(h.conversation(fork).unwrap().messages.len(), 2);
    assert!(h.fork_by_entry_id(child, inherited).is_err());
}

#[tokio::test]
async fn background_abort_reaches_reused_child_through_terminal_anchor_after_reopen() {
    use crate::task_store::TaskStore;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let config = Config {
        model: "test".into(),
        system_prompt: String::new(),
        max_output_tokens: 10,
    };
    let parent = h.create_conversation(config.clone()).unwrap();
    let other = h.create_conversation(config.clone()).unwrap();
    let child = h
        .ensure_subagent(parent, "worker", config.clone())
        .unwrap()
        .child;
    let unrelated = h.ensure_subagent(other, "worker", config).unwrap().child;
    let anchor = h.anchor_subagent(parent, "worker").unwrap();
    let store = TaskStore::new(h.owner.store());
    assert!(matches!(
        store.load(anchor).unwrap().unwrap().task.state(),
        crate::task::TaskState::Succeeded(serde_json::Value::Null)
    ));
    h.submit(child, None, "first".into()).unwrap();
    h.submit(child, None, "queued".into()).unwrap();
    h.request_abort(parent).unwrap();
    assert!(!h.conversation(child).unwrap().abort_requested);
    h.request_abort_with_background(parent).unwrap();
    h.request_abort_with_background(parent).unwrap();
    assert!(h.conversation(child).unwrap().abort_requested);
    assert!(!h.conversation(unrelated).unwrap().abort_requested);
    assert!(matches!(
        h.conversation(child).unwrap().submissions[1].status,
        Status::Unanswered { .. }
    ));
    assert!(
        TaskStore::new(h.owner.store())
            .load(anchor)
            .unwrap()
            .unwrap()
            .task
            .is_terminal()
    );
    drop(h);
    let mut h = Harness::open_paused(&path, options()).unwrap();
    assert!(h.conversation(child).unwrap().abort_requested);
    h.drive(child, &CancellationToken::new()).await.unwrap();
    h.drive(parent, &CancellationToken::new()).await.unwrap();
    assert_eq!(h.anchor_subagent(parent, "worker").unwrap(), anchor);
    h.submit(child, None, "reused".into()).unwrap();
    h.request_abort_with_background(parent).unwrap();
    assert!(h.conversation(child).unwrap().abort_requested);
    assert!(!h.conversation(unrelated).unwrap().abort_requested);
}

#[tokio::test]
async fn background_abort_reports_missing_driver_while_foreground_cleanup_waits() {
    use crate::task::{Checkpoint, Task, TaskOwner};
    use crate::task_store::{Save, TaskStore};
    let dir = tempfile::tempdir().unwrap();
    let mut scheduler = Scheduler::open(dir.path().join("db"), options()).unwrap();
    let id = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: String::new(),
            max_output_tokens: 10,
        })
        .unwrap();
    let task = Task::new(
        Uuid::new_v4(),
        TaskOwner::Conversation(id),
        Checkpoint {
            phase: "initial".into(),
            data: serde_json::Value::Null,
        },
        3,
    )
    .unwrap()
    .registered("job".into(), true);
    TaskStore::new(scheduler.harness.owner.store())
        .save(&[Save::Create(&task)])
        .unwrap();
    for _ in 0..2 {
        let result = tokio::time::timeout(
            Duration::from_secs(2),
            scheduler.abort_and_wait_with_background(id),
        )
        .await
        .expect("abort must not hang");
        assert!(
            matches!(result, Err(Error::Invalid(message)) if message.contains("task driver is not running"))
        );
        assert!(
            TaskStore::new(scheduler.harness.owner.store())
                .load(task.id())
                .unwrap()
                .unwrap()
                .task
                .abort_requested()
        );
        assert!(scheduler.conversation(id).unwrap().abort_requested);
    }
    let mut registry = crate::task_scheduler::Registry::default();
    registry.register_with_abort(
        "job",
        true,
        |_: &Task| {
            Ok(crate::task_scheduler::Step::Succeed(
                serde_json::Value::Null,
            ))
        },
        |_: &Task| {
            Ok(crate::task_scheduler::Step::Succeed(
                serde_json::Value::Null,
            ))
        },
    );
    let driver = scheduler.start_tasks(registry).unwrap();
    tokio::time::timeout(
        Duration::from_secs(5),
        scheduler.abort_and_wait_with_background(id),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(driver.get(task.id()).unwrap().unwrap().is_terminal());
    assert!(!scheduler.conversation(id).unwrap().abort_requested);
    driver.shutdown().unwrap();
    scheduler.shutdown().await.unwrap();
}

#[tokio::test]
async fn background_abort_reports_driver_shutdown_during_foreground_cleanup() {
    use crate::task::{Checkpoint, Task, TaskOwner};
    use crate::task_scheduler::{Registry, Step};
    let dir = tempfile::tempdir().unwrap();
    let mut scheduler = Scheduler::open(dir.path().join("db"), options()).unwrap();
    let id = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: String::new(),
            max_output_tokens: 10,
        })
        .unwrap();
    let task = Task::new(
        Uuid::new_v4(),
        TaskOwner::Conversation(id),
        Checkpoint {
            phase: "initial".into(),
            data: serde_json::Value::Null,
        },
        3,
    )
    .unwrap()
    .registered("job".into(), true);
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let release_rx = std::sync::Mutex::new(release_rx);
    let mut registry = Registry::default();
    registry.register_with_abort(
        "job",
        true,
        move |_: &Task| {
            started_tx.send(()).unwrap();
            release_rx.lock().unwrap().recv().unwrap();
            Ok(Step::Succeed(serde_json::Value::Null))
        },
        |_: &Task| Ok(Step::Succeed(serde_json::Value::Null)),
    );
    let driver = scheduler.start_tasks(registry).unwrap();
    driver.submit(&task).unwrap();
    started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    let stopper = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(250));
        drop(driver);
    });
    let result = tokio::time::timeout(
        Duration::from_secs(3),
        scheduler.abort_and_wait_with_background(id),
    )
    .await
    .expect("abort must notice shutdown");
    assert!(
        matches!(result, Err(Error::Invalid(message)) if message.contains("task driver stopped") || message.contains("task driver is not running"))
    );
    assert!(scheduler.conversation(id).unwrap().abort_requested);
    release_tx.send(()).unwrap();
    stopper.join().unwrap();
    scheduler.shutdown().await.unwrap();
}
#[test]
fn failed_policy_retries_with_latest_setting_and_inbox() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut owner = Harness::open_paused(&path, options()).unwrap();
    let id = owner
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 10,
        })
        .unwrap();
    owner.submit(id, None, "active".into()).unwrap();
    let existing = owner.submit(id, None, "existing".into()).unwrap();
    let mut worker = owner.worker();
    let (mut stale, version) = worker.load(id).unwrap();
    finish_failure(&mut stale, "failure".into());
    owner.withdraw_submission(id, existing).unwrap();
    let added = owner.submit(id, None, "added".into()).unwrap();
    owner
        .set_after_failed_run(id, AfterFailedRun::Withdraw)
        .unwrap();
    worker.save(&stale, Expected::Version(version)).unwrap();
    let c = owner.conversation(id).unwrap();
    assert_eq!(
        c.submissions
            .iter()
            .find(|s| s.id == existing)
            .unwrap()
            .status,
        Status::Unanswered {
            reason: "aborted".into()
        }
    );
    assert_eq!(
        c.submissions.iter().find(|s| s.id == added).unwrap().status,
        Status::Unanswered {
            reason: "run_failed".into()
        }
    );
}

#[tokio::test]
async fn continue_failure_boundary_survives_restart_and_historical_forks() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let id = h
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 10,
        })
        .unwrap();
    let before = h
        .history(
            id,
            crate::observer::Cursor(0),
            std::num::NonZeroUsize::new(100).unwrap(),
        )
        .unwrap()
        .records
        .last()
        .unwrap()
        .cursor;
    h.set_after_failed_run(id, AfterFailedRun::Continue)
        .unwrap();
    let configured = h
        .history(id, before, std::num::NonZeroUsize::new(100).unwrap())
        .unwrap()
        .records
        .last()
        .unwrap()
        .cursor;
    h.submit(id, None, "active".into()).unwrap();
    let queued = h.submit(id, None, "queued".into()).unwrap();
    let (mut state, version) = h.load(id).unwrap();
    finish_failure(&mut state, "failure".into());
    h.save(&state, Expected::Version(version)).unwrap();
    assert!(matches!(h.conversation(id).unwrap().phase, Phase::Idle));
    let old_fork = h.fork_at(id, before).unwrap();
    let configured_fork = h.fork_at(id, configured).unwrap();
    assert_eq!(
        h.conversation(old_fork).unwrap().after_failed_run,
        AfterFailedRun::Keep
    );
    assert_eq!(
        h.conversation(configured_fork).unwrap().after_failed_run,
        AfterFailedRun::Continue
    );
    drop(h);
    let mut h = Harness::open_paused(&path, options()).unwrap();
    assert_eq!(
        h.conversation(id)
            .unwrap()
            .submissions
            .iter()
            .find(|s| s.id == queued)
            .unwrap()
            .status,
        Status::Queued
    );
    h.resume_conversation(id, &CancellationToken::new())
        .await
        .unwrap();
    assert!(matches!(
        h.conversation(id)
            .unwrap()
            .submissions
            .iter()
            .find(|s| s.id == queued)
            .unwrap()
            .status,
        Status::Done { .. }
    ));
}

#[tokio::test]
async fn abort_wins_over_stale_failure_and_preserves_later_admissions() {
    let dir = tempfile::tempdir().unwrap();
    let mut owner = Harness::open_paused(dir.path().join("db"), options()).unwrap();
    let id = owner
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 10,
        })
        .unwrap();
    owner
        .set_after_failed_run(id, AfterFailedRun::Withdraw)
        .unwrap();
    owner.submit(id, None, "active".into()).unwrap();
    let queued = owner.submit(id, None, "before abort".into()).unwrap();
    let mut worker = owner.worker();
    let (mut stale, version) = worker.load(id).unwrap();
    finish_failure(&mut stale, "failure".into());
    owner.request_abort(id).unwrap();
    let later = owner.submit(id, None, "after abort".into()).unwrap();
    assert!(worker.save(&stale, Expected::Version(version)).is_err());
    let c = owner.conversation(id).unwrap();
    assert!(c.abort_requested);
    assert_eq!(
        c.submissions
            .iter()
            .find(|s| s.id == queued)
            .unwrap()
            .status,
        Status::Unanswered {
            reason: "aborted".into()
        }
    );
    owner
        .resume_conversation(id, &CancellationToken::new())
        .await
        .unwrap();
    assert!(matches!(
        owner
            .conversation(id)
            .unwrap()
            .submissions
            .iter()
            .find(|s| s.id == later)
            .unwrap()
            .status,
        Status::Done { .. }
    ));
}

#[test]
fn concurrent_managed_first_admissions_and_interrupted_delivery_reopen() {
    use crate::subagent::ReportedInput;
    use crate::task_store::TaskStore;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut owner = Harness::open_paused(&path, options()).unwrap();
    let config = Config {
        model: "test".into(),
        system_prompt: String::new(),
        max_output_tokens: 10,
    };
    let parent = owner.create_conversation(config.clone()).unwrap();
    let barrier = Arc::new(std::sync::Barrier::new(8));
    let threads: Vec<_> = (0..8)
        .map(|_| {
            let mut worker = owner.worker();
            let barrier = barrier.clone();
            let config = config.clone();
            std::thread::spawn(move || {
                barrier.wait();
                worker
                    .ensure_managed_subagent(parent, "worker", config, None)
                    .unwrap()
            })
        })
        .collect();
    let links: Vec<_> = threads.into_iter().map(|t| t.join().unwrap()).collect();
    assert!(links.iter().all(|link| link == &links[0]));
    let child = links[0].child;
    let anchor = owner.anchor_subagent(parent, "worker").unwrap();
    assert!(matches!(
        TaskStore::new(owner.owner.store())
            .load(anchor)
            .unwrap()
            .unwrap()
            .task
            .state(),
        crate::task::TaskState::Succeeded(serde_json::Value::Null)
    ));
    assert_eq!(
        owner
            .owner
            .store()
            .list(b"durable/conversation-owner/")
            .unwrap()
            .len(),
        1
    );
    // Simulate termination after atomic creation and before reporter admission.
    drop(owner);
    let mut owner = Harness::open_paused(&path, options()).unwrap();
    assert_eq!(
        owner
            .ensure_managed_subagent(parent, "worker", config.clone(), None)
            .unwrap()
            .child,
        child
    );
    assert_eq!(owner.anchor_subagent(parent, "worker").unwrap(), anchor);
    let submit = |h: &mut Harness| {
        h.submit_subagent_reported(
            parent,
            "worker",
            config.clone(),
            ReportedInput {
                history: false,
                request_id: "same".into(),
                content: "hello".into(),
                mode: InputMode::FollowUp,
            },
        )
        .unwrap()
        .1
    };
    assert_eq!(submit(&mut owner), submit(&mut owner));
    assert_eq!(owner.conversation(child).unwrap().submissions.len(), 1);
}

#[test]
fn managed_anchor_admission_is_fenced_by_parent_abort() {
    let dir = tempfile::tempdir().unwrap();
    let mut h = Harness::open_paused(dir.path().join("db"), options()).unwrap();
    let config = Config {
        model: "test".into(),
        system_prompt: String::new(),
        max_output_tokens: 10,
    };
    let parent = h.create_conversation(config.clone()).unwrap();
    h.submit(parent, None, "work".into()).unwrap();
    h.request_abort_with_background(parent).unwrap();
    assert!(h.conversation(parent).unwrap().abort_requested);
    assert!(
        h.ensure_managed_subagent(parent, "worker", config, None)
            .is_err()
    );
    assert!(h.subagent(parent, "worker").unwrap().is_none());
    assert!(
        h.owner
            .store()
            .list(b"durable/conversation-owner/")
            .unwrap()
            .is_empty()
    );
    assert!(
        h.owner
            .store()
            .list(b"runtime/subagent-anchor/")
            .unwrap()
            .is_empty()
    );
}

#[test]
fn existing_managed_anchor_rejects_admission_during_parent_abort() {
    let dir = tempfile::tempdir().unwrap();
    let mut h = Harness::open_paused(dir.path().join("db"), options()).unwrap();
    let config = Config {
        model: "test".into(),
        system_prompt: String::new(),
        max_output_tokens: 10,
    };
    let parent = h.create_conversation(config.clone()).unwrap();
    let link = h
        .ensure_managed_subagent(parent, "worker", config.clone(), None)
        .unwrap();
    h.request_abort_with_background(parent).unwrap();
    assert!(h.conversation(link.child).unwrap().abort_requested);
    assert!(
        h.ensure_managed_subagent(parent, "worker", config, None)
            .is_err()
    );
    assert!(h.conversation(link.child).unwrap().submissions.is_empty());
}

#[test]
fn managed_anchor_races_background_abort_without_stranding_child() {
    for _ in 0..12 {
        let dir = tempfile::tempdir().unwrap();
        let mut h = Harness::open_paused(dir.path().join("db"), options()).unwrap();
        let config = Config {
            model: "test".into(),
            system_prompt: String::new(),
            max_output_tokens: 10,
        };
        let parent = h.create_conversation(config.clone()).unwrap();
        h.submit(parent, None, "work".into()).unwrap();
        let mut admission = h.worker();
        let mut abort = h.worker();
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let other = barrier.clone();
        let thread = std::thread::spawn(move || {
            other.wait();
            admission.ensure_managed_subagent(parent, "worker", config, None)
        });
        barrier.wait();
        abort.request_abort_with_background(parent).unwrap();
        match thread.join().unwrap() {
            Ok(link) => assert!(h.conversation(link.child).unwrap().abort_requested),
            Err(_) => assert!(h.subagent(parent, "worker").unwrap().is_none()),
        }
    }
}

#[test]
fn managed_anchor_kill_helper() {
    let Ok(path) = std::env::var("PI_RS_ANCHOR_KILL_DB") else {
        return;
    };
    let config = Config {
        model: "test".into(),
        system_prompt: String::new(),
        max_output_tokens: 10,
    };
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let parent = h.create_conversation(config.clone()).unwrap();
    let child = h
        .ensure_managed_subagent(parent, "worker", config, None)
        .unwrap()
        .child;
    let anchor = h.anchor_subagent(parent, "worker").unwrap();
    std::fs::write(
        format!("{path}.pending"),
        format!("{parent} {child} {anchor}"),
    )
    .unwrap();
    std::fs::rename(format!("{path}.pending"), format!("{path}.ready")).unwrap();
    loop {
        std::thread::park();
    }
}

#[test]
fn managed_anchor_survives_process_kill_before_delivery() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let ready = dir.path().join("db.ready");
    let mut process = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "runtime::runtime_cas_tests::managed_anchor_kill_helper",
        ])
        .env("PI_RS_ANCHOR_KILL_DB", &path)
        .spawn()
        .unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while !ready.exists() && std::time::Instant::now() < deadline {
        assert!(process.try_wait().unwrap().is_none(), "helper exited early");
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    process.kill().unwrap();
    process.wait().unwrap();
    let contents = std::fs::read_to_string(ready).expect("helper did not commit in time");
    let ids: Vec<Uuid> = contents
        .split_whitespace()
        .map(|s| s.parse().unwrap())
        .collect();
    let mut h = Harness::open_paused(&path, options()).unwrap();
    assert_eq!(h.subagent(ids[0], "worker").unwrap().unwrap().child, ids[1]);
    assert_eq!(h.anchor_subagent(ids[0], "worker").unwrap(), ids[2]);
    assert!(h.conversation(ids[1]).unwrap().submissions.is_empty());
}
