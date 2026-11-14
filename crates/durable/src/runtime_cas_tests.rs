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
    assert!(h.fork_at_position(id, entries[1].position.clone()).is_err());
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
