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
fn abort_returns_queued_inputs_across_foreground_and_background_boundaries() {
    use crate::ownership_edge::Mode;
    use crate::task::{Checkpoint, Task, TaskOwner};
    use crate::task_store::{Save, TaskStore};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let config = Config {
        model: "test".into(),
        system_prompt: String::new(),
        max_output_tokens: 10,
    };
    let parent = h.create_conversation(config.clone()).unwrap();
    let owner = TaskOwner::Conversation(parent);
    let root = Task::new(
        Uuid::new_v4(),
        owner.clone(),
        Checkpoint {
            phase: "start".into(),
            data: serde_json::Value::Null,
        },
        3,
    )
    .unwrap();
    TaskStore::new(h.owner.store())
        .save(&[Save::Create(&root)])
        .unwrap();
    let foreground = h
        .ensure_task_owned_subagent(
            root.id(),
            owner.clone(),
            "fg",
            config.clone(),
            Mode::Foreground,
        )
        .unwrap()
        .child;
    let background = h
        .ensure_task_owned_subagent(root.id(), owner, "bg", config, Mode::Background)
        .unwrap()
        .child;
    for id in [parent, foreground, background] {
        h.submit(id, None, "running".into()).unwrap();
        h.submit_with_mode(id, None, "queued".into(), InputMode::Steer)
            .unwrap();
    }
    let (_, _, result) = h
        .abort_owned_graph_impl(Some(parent), None, None, false)
        .unwrap();
    assert_eq!(result.withdrawn.len(), 2);
    for id in [parent, foreground] {
        let item = result
            .withdrawn
            .iter()
            .find(|item| item.conversation_id == id)
            .unwrap();
        assert_eq!(item.mode, InputMode::Steer);
        assert_eq!(item.submission.content, "queued");
        assert_eq!(item.submission.status, Status::Queued);
        assert_eq!(
            h.conversation(id).unwrap().submissions[1].status,
            Status::Unanswered {
                reason: "aborted".into()
            }
        );
    }
    assert_eq!(
        h.conversation(background).unwrap().submissions[1].status,
        Status::Queued
    );
    let (_, _, repeat) = h
        .abort_owned_graph_impl(Some(parent), None, None, false)
        .unwrap();
    assert!(repeat.withdrawn.is_empty());
    let (_, _, include_background) = h
        .abort_owned_graph_impl(Some(parent), None, None, true)
        .unwrap();
    assert_eq!(include_background.withdrawn.len(), 1);
    assert_eq!(include_background.withdrawn[0].conversation_id, background);
    drop(h);
    let reopened = Harness::open_paused(&path, options()).unwrap();
    for id in [parent, foreground, background] {
        assert_eq!(
            reopened.conversation(id).unwrap().submissions[1].status,
            Status::Unanswered {
                reason: "aborted".into()
            }
        );
    }
}

#[test]
fn abort_result_retries_when_an_input_arrives_before_commit() {
    let dir = tempfile::tempdir().unwrap();
    let mut h = Harness::open_paused(dir.path().join("db"), options()).unwrap();
    let id = h
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: String::new(),
            max_output_tokens: 10,
        })
        .unwrap();
    h.submit(id, None, "running".into()).unwrap();
    h.submit(id, None, "before".into()).unwrap();
    let mut other = h.worker();
    let mut injected = false;
    let (_, _, result) = h
        .abort_owned_graph_impl_with(Some(id), None, None, false, || {
            if !injected {
                injected = true;
                other.submit(id, None, "raced".into()).unwrap();
            }
        })
        .unwrap();
    assert!(injected);
    assert_eq!(
        result
            .withdrawn
            .iter()
            .map(|item| item.submission.content.as_str())
            .collect::<Vec<_>>(),
        vec!["before", "raced"]
    );
    let state = h.conversation(id).unwrap();
    assert!(state.submissions[1..].iter().all(|item| item.status
        == Status::Unanswered {
            reason: "aborted".into()
        }));
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
fn queued_write_withdrawal_wins_over_stale_reset_placement_and_survives_reopen() {
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
    let reset = owner.reset(id, None).unwrap();
    let write = owner
        .submit_write(id, Some("write-key".into()), "app.note".into(), None)
        .unwrap();
    let mut worker = owner.worker();
    let (mut stale, version) = worker.load(id).unwrap();
    let marker = stale.pending_resets.remove(0);
    place_reset(&mut stale, marker);
    stale.phase = Phase::Idle;
    assert_eq!(
        owner.withdraw_submission(id, write).unwrap(),
        WithdrawResult::Aborted
    );
    worker.save(&stale, Expected::Version(version)).unwrap();
    let state = owner.conversation(id).unwrap();
    assert_eq!(state.resets.last().unwrap().id, reset);
    assert!(state.application_writes[0].withdrawn);
    assert!(!state.application_writes[0].placed);
    drop(worker);
    drop(owner);
    let mut reopened = Harness::open_paused(&path, options()).unwrap();
    assert_eq!(
        reopened.withdraw_submission(id, write).unwrap(),
        WithdrawResult::Settled
    );
    assert_eq!(
        reopened
            .submit_write(id, Some("write-key".into()), "app.other".into(), None)
            .unwrap(),
        write
    );
    assert!(
        reopened
            .submit(id, Some("write-key".into()), "input".into())
            .is_err()
    );
    let state = reopened.conversation(id).unwrap();
    assert!(state.application_writes[0].withdrawn && !state.application_writes[0].placed);
}

#[test]
fn committed_write_placement_wins_over_stale_withdrawal() {
    let dir = tempfile::tempdir().unwrap();
    let mut owner = Harness::open_paused(dir.path().join("db"), options()).unwrap();
    let id = owner
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 10,
        })
        .unwrap();
    owner.submit(id, None, "active".into()).unwrap();
    owner.reset(id, None).unwrap();
    let write = owner
        .submit_write(id, None, "app.note".into(), None)
        .unwrap();
    let mut worker = owner.worker();
    let (mut stale, version) = worker.load(id).unwrap();
    stale.application_writes[0].withdrawn = true;
    let (mut placed, current) = owner.load(id).unwrap();
    let marker = placed.pending_resets.remove(0);
    place_reset(&mut placed, marker);
    owner.save(&placed, Expected::Version(current)).unwrap();
    worker.merge_admissions = false;
    assert!(matches!(
        worker.save(&stale, Expected::Version(version)),
        Err(Error::Storage(durable_store::Error::Conflict { .. }))
    ));
    assert_eq!(
        worker.withdraw_submission(id, write).unwrap(),
        WithdrawResult::Settled
    );
    let state = owner.conversation(id).unwrap();
    assert!(state.application_writes[0].placed && !state.application_writes[0].withdrawn);
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
    finish_failure(&mut state, "failure".into(), FailureCause::ModelError);
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
fn low_level_creation_committing_after_scan_retries_abort_and_survives_reopen() {
    use crate::task::{Checkpoint, Task, TaskOwner};
    use crate::task_store::{Save, TaskStore};
    use serde_json::json;
    use std::sync::mpsc;
    use std::time::Duration;
    // Already marked and terminal task owners, a direct conversation owner,
    // an atomic host checkpoint, and an independent background branch.
    for case in 0..5 {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut h = Harness::open_paused(&path, options()).unwrap();
        let conversation = h
            .create_conversation(Config {
                model: "test".into(),
                system_prompt: String::new(),
                max_output_tokens: 10,
            })
            .unwrap();
        let mut parent = Task::new(
            Uuid::new_v4(),
            TaskOwner::Conversation(conversation),
            Checkpoint {
                phase: "start".into(),
                data: json!(null),
            },
            4,
        )
        .unwrap();
        if case == 4 {
            parent = parent.background().unwrap();
        }
        if case == 1 {
            parent.start().unwrap();
            parent.succeed(json!(null)).unwrap();
        } else {
            parent.request_abort().unwrap();
        }
        TaskStore::new(h.subagent_store().unwrap())
            .save(&[Save::Create(&parent)])
            .unwrap();
        let parent_version = TaskStore::new(h.subagent_store().unwrap())
            .load(parent.id())
            .unwrap()
            .unwrap()
            .version;
        let owner = if case == 2 {
            TaskOwner::Conversation(conversation)
        } else {
            TaskOwner::Task(parent.id())
        };
        let make_child = || {
            Task::new(
                Uuid::new_v4(),
                owner.clone(),
                Checkpoint {
                    phase: "child".into(),
                    data: json!(null),
                },
                4,
            )
            .unwrap()
            .registered("test".into(), true)
        };
        let children = [make_child(), make_child()];
        let ids = [children[0].id(), children[1].id()];
        let (ready_tx, ready_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();
        let writer = h.worker();
        let thread = std::thread::spawn(move || {
            ready_rx.recv_timeout(Duration::from_secs(10)).unwrap();
            let store = writer.subagent_store().unwrap();
            let tasks = TaskStore::new(store);
            let batch = if case == 3 {
                tasks
                    .prepare_owned_tasks(
                        &owner,
                        b"host/checkpoint",
                        Expected::Missing,
                        b"atomic",
                        &children,
                    )
                    .unwrap()
            } else {
                tasks
                    .prepare_save(&[Save::Create(&children[0]), Save::Create(&children[1])])
                    .unwrap()
            };
            store.commit(&batch.checks, &batch.mutations).unwrap();
            done_tx.send(()).unwrap();
        });
        let mut attempts = 0;
        h.abort_owned_graph_impl_with(Some(conversation), None, None, false, || {
            attempts += 1;
            if attempts == 1 {
                ready_tx.send(()).unwrap();
                done_rx.recv_timeout(Duration::from_secs(10)).unwrap();
            }
        })
        .unwrap();
        thread.join().unwrap();
        assert_eq!(attempts, 2, "abort must rescan a newly committed child");
        let ts = TaskStore::new(h.subagent_store().unwrap());
        assert_eq!(
            ts.load(parent.id()).unwrap().unwrap().version,
            parent_version
        );
        for id in ids {
            assert_eq!(
                ts.load(id).unwrap().unwrap().task.abort_requested(),
                case != 4
            );
        }
        if case == 3 {
            assert_eq!(
                h.subagent_store()
                    .unwrap()
                    .get(b"host/checkpoint")
                    .unwrap()
                    .unwrap()
                    .value,
                b"atomic"
            );
        }
        drop(h);
        let reopened = Harness::open_paused(&path, options()).unwrap();
        for id in ids {
            assert_eq!(
                TaskStore::new(reopened.subagent_store().unwrap())
                    .load(id)
                    .unwrap()
                    .unwrap()
                    .task
                    .abort_requested(),
                case != 4
            );
        }
        assert!(reopened.conversation(conversation).unwrap().abort_requested);
    }
}

#[test]
fn prepared_low_level_children_are_fenced_after_abort_without_rewriting_owner() {
    use crate::task::{Checkpoint, Task, TaskOwner};
    use crate::task_store::{Save, TaskStore};
    use serde_json::json;
    use std::sync::mpsc;
    use std::time::Duration;
    // Marked and terminal task anchors, conversation root, and the checkpoint
    // composing API all retain their existing CAS conditions.
    for case in 0..4 {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut h = Harness::open_paused(&path, options()).unwrap();
        let conversation = h
            .create_conversation(Config {
                model: "test".into(),
                system_prompt: String::new(),
                max_output_tokens: 10,
            })
            .unwrap();
        let mut anchor = Task::new(
            Uuid::new_v4(),
            TaskOwner::Conversation(conversation),
            Checkpoint {
                phase: "start".into(),
                data: json!(null),
            },
            4,
        )
        .unwrap();
        if case == 0 || case == 2 || case == 3 {
            anchor.request_abort().unwrap();
        }
        if case == 1 {
            anchor.start().unwrap();
            anchor.succeed(json!(null)).unwrap();
        }
        let store = h.subagent_store().unwrap();
        TaskStore::new(store)
            .save(&[Save::Create(&anchor)])
            .unwrap();
        let version = TaskStore::new(store)
            .load(anchor.id())
            .unwrap()
            .unwrap()
            .version;
        let owner = if case == 2 {
            TaskOwner::Conversation(conversation)
        } else {
            TaskOwner::Task(anchor.id())
        };
        let child = Task::new(
            Uuid::new_v4(),
            owner.clone(),
            Checkpoint {
                phase: "child".into(),
                data: json!(null),
            },
            4,
        )
        .unwrap()
        .registered("test".into(), true);
        let child_id = child.id();
        let (ready_tx, ready_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let writer = h.worker();
        let thread = std::thread::spawn(move || {
            let store = writer.subagent_store().unwrap();
            let tasks = TaskStore::new(store);
            let batch = if case == 3 {
                tasks
                    .prepare_owned_tasks(
                        &owner,
                        b"host/checkpoint",
                        Expected::Missing,
                        b"new",
                        &[child],
                    )
                    .unwrap()
            } else {
                tasks.prepare_save(&[Save::Create(&child)]).unwrap()
            };
            ready_tx.send(()).unwrap();
            release_rx.recv_timeout(Duration::from_secs(10)).unwrap();
            store.commit(&batch.checks, &batch.mutations)
        });
        ready_rx.recv_timeout(Duration::from_secs(10)).unwrap();
        h.request_abort(conversation).unwrap();
        assert_eq!(
            TaskStore::new(h.subagent_store().unwrap())
                .load(anchor.id())
                .unwrap()
                .unwrap()
                .version,
            version,
            "unchanged task reservations must not be rewritten"
        );
        release_tx.send(()).unwrap();
        assert!(matches!(
            thread.join().unwrap(),
            Err(durable_store::Error::Conflict { .. })
        ));
        assert!(
            TaskStore::new(h.subagent_store().unwrap())
                .load(child_id)
                .unwrap()
                .is_none()
        );
        assert!(
            h.subagent_store()
                .unwrap()
                .get(b"host/checkpoint")
                .unwrap()
                .is_none()
        );
        drop(h);
        let reopened = Harness::open_paused(&path, options()).unwrap();
        assert!(
            TaskStore::new(reopened.subagent_store().unwrap())
                .load(child_id)
                .unwrap()
                .is_none()
        );
    }
}

#[test]
fn prepared_ownership_fence_survives_reopen() {
    use crate::task::{Checkpoint, Task, TaskOwner};
    use crate::task_store::{Save, TaskStore};
    use serde_json::json;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let conversation = h
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: String::new(),
            max_output_tokens: 10,
        })
        .unwrap();
    let mut parent = Task::new(
        Uuid::new_v4(),
        TaskOwner::Conversation(conversation),
        Checkpoint {
            phase: "start".into(),
            data: json!(null),
        },
        4,
    )
    .unwrap();
    parent.request_abort().unwrap();
    let child = Task::new(
        Uuid::new_v4(),
        TaskOwner::Task(parent.id()),
        Checkpoint {
            phase: "child".into(),
            data: json!(null),
        },
        4,
    )
    .unwrap();
    let store = h.subagent_store().unwrap();
    TaskStore::new(store)
        .save(&[Save::Create(&parent)])
        .unwrap();
    let batch = TaskStore::new(store)
        .prepare_save(&[Save::Create(&child)])
        .unwrap();
    h.request_abort(conversation).unwrap();
    drop(h);
    let reopened = Harness::open_paused(&path, options()).unwrap();
    let store = reopened.subagent_store().unwrap();
    assert!(matches!(
        store.commit(&batch.checks, &batch.mutations),
        Err(durable_store::Error::Conflict { .. })
    ));
    assert!(TaskStore::new(store).load(child.id()).unwrap().is_none());
}

#[test]
fn prepared_conversation_edges_keep_cas_and_background_boundaries() {
    use crate::ownership_edge::{Edge, EdgeStore, Mode};
    use crate::task::{Checkpoint, Task, TaskOwner};
    use crate::task_store::{Save, TaskStore};
    use serde_json::json;
    use std::sync::mpsc;
    use std::time::Duration;
    for background in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let mut h = Harness::open_paused(dir.path().join("db"), options()).unwrap();
        let config = || Config {
            model: "test".into(),
            system_prompt: String::new(),
            max_output_tokens: 10,
        };
        let conversation = h.create_conversation(config()).unwrap();
        let child = h.create_conversation(config()).unwrap();
        let mut task = Task::new(
            Uuid::new_v4(),
            TaskOwner::Conversation(conversation),
            Checkpoint {
                phase: "start".into(),
                data: json!(null),
            },
            4,
        )
        .unwrap();
        if background {
            task = task.background().unwrap();
        }
        TaskStore::new(h.subagent_store().unwrap())
            .save(&[Save::Create(&task)])
            .unwrap();
        let edge = Edge {
            task: task.id(),
            task_owner: TaskOwner::Conversation(conversation),
            child,
            mode: Mode::Foreground,
        };
        let writer = h.worker();
        let (ready_tx, ready_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let thread = std::thread::spawn(move || {
            let store = writer.subagent_store().unwrap();
            let batch = EdgeStore::new(store).prepare_create(&edge).unwrap();
            ready_tx.send(()).unwrap();
            release_rx.recv_timeout(Duration::from_secs(10)).unwrap();
            store.commit(&batch.checks, &batch.mutations)
        });
        ready_rx.recv_timeout(Duration::from_secs(10)).unwrap();
        h.request_abort(conversation).unwrap();
        release_tx.send(()).unwrap();
        let result = thread.join().unwrap();
        if background {
            result.unwrap();
            assert!(
                EdgeStore::new(h.subagent_store().unwrap())
                    .get(child)
                    .unwrap()
                    .is_some()
            );
        } else {
            assert!(matches!(result, Err(durable_store::Error::Conflict { .. })));
            assert!(
                EdgeStore::new(h.subagent_store().unwrap())
                    .get(child)
                    .unwrap()
                    .is_none()
            );
        }
    }
}

#[test]
fn prepared_background_owned_child_survives_ordinary_parent_abort() {
    use crate::task::{Checkpoint, Task, TaskOwner};
    use crate::task_store::{Save, TaskStore};
    use serde_json::json;
    let dir = tempfile::tempdir().unwrap();
    let mut h = Harness::open_paused(dir.path().join("db"), options()).unwrap();
    let conversation = h
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: String::new(),
            max_output_tokens: 10,
        })
        .unwrap();
    let background = Task::new(
        Uuid::new_v4(),
        TaskOwner::Conversation(conversation),
        Checkpoint {
            phase: "start".into(),
            data: json!(null),
        },
        4,
    )
    .unwrap()
    .background()
    .unwrap();
    let child = Task::new(
        Uuid::new_v4(),
        TaskOwner::Task(background.id()),
        Checkpoint {
            phase: "child".into(),
            data: json!(null),
        },
        4,
    )
    .unwrap();
    let store = h.subagent_store().unwrap();
    TaskStore::new(store)
        .save(&[Save::Create(&background)])
        .unwrap();
    let batch = TaskStore::new(store)
        .prepare_save(&[Save::Create(&child)])
        .unwrap();
    h.request_abort(conversation).unwrap();
    let store = h.subagent_store().unwrap();
    store.commit(&batch.checks, &batch.mutations).unwrap();
    assert!(
        !TaskStore::new(store)
            .load(child.id())
            .unwrap()
            .unwrap()
            .task
            .abort_requested()
    );
}

#[test]
fn background_inclusive_abort_fences_prepared_background_child() {
    use crate::task::{Checkpoint, Task, TaskOwner};
    use crate::task_store::{Save, TaskStore};
    use serde_json::json;
    use std::sync::mpsc;
    use std::time::Duration;
    let dir = tempfile::tempdir().unwrap();
    let mut h = Harness::open_paused(dir.path().join("db"), options()).unwrap();
    let conversation = h
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: String::new(),
            max_output_tokens: 10,
        })
        .unwrap();
    let mut background = Task::new(
        Uuid::new_v4(),
        TaskOwner::Conversation(conversation),
        Checkpoint {
            phase: "start".into(),
            data: json!(null),
        },
        4,
    )
    .unwrap()
    .background()
    .unwrap();
    background.request_abort().unwrap();
    let store = h.subagent_store().unwrap();
    TaskStore::new(store)
        .save(&[Save::Create(&background)])
        .unwrap();
    let version = TaskStore::new(store)
        .load(background.id())
        .unwrap()
        .unwrap()
        .version;
    let child = Task::new(
        Uuid::new_v4(),
        TaskOwner::Task(background.id()),
        Checkpoint {
            phase: "child".into(),
            data: json!(null),
        },
        4,
    )
    .unwrap();
    let child_id = child.id();
    let writer = h.worker();
    let (ready_tx, ready_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel();
    let thread = std::thread::spawn(move || {
        let store = writer.subagent_store().unwrap();
        let batch = TaskStore::new(store)
            .prepare_save(&[Save::Create(&child)])
            .unwrap();
        ready_tx.send(()).unwrap();
        release_rx.recv_timeout(Duration::from_secs(10)).unwrap();
        store.commit(&batch.checks, &batch.mutations)
    });
    ready_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    h.request_abort_with_background(conversation).unwrap();
    assert_eq!(
        TaskStore::new(h.subagent_store().unwrap())
            .load(background.id())
            .unwrap()
            .unwrap()
            .version,
        version
    );
    release_tx.send(()).unwrap();
    assert!(matches!(
        thread.join().unwrap(),
        Err(durable_store::Error::Conflict { .. })
    ));
    assert!(
        TaskStore::new(h.subagent_store().unwrap())
            .load(child_id)
            .unwrap()
            .is_none()
    );
}

#[test]
fn cyclic_abort_retries_if_unchanged_task_changes_before_commit() {
    use crate::ownership_edge::{self, Edge, Mode};
    use crate::task::{Checkpoint, Task, TaskOwner};
    use crate::task_store::{Save, TaskStore};
    use serde_json::json;
    use std::sync::mpsc;
    use std::time::Duration;

    for terminal in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let mut h = Harness::open_paused(&path, options()).unwrap();
        let conversation = h
            .create_conversation(Config {
                model: "test".into(),
                system_prompt: String::new(),
                max_output_tokens: 10,
            })
            .unwrap();
        let task = Task::new(
            Uuid::new_v4(),
            TaskOwner::Conversation(conversation),
            Checkpoint {
                phase: "initial".into(),
                data: json!(null),
            },
            4,
        )
        .unwrap();
        let mut old_task = task.clone();
        if terminal {
            old_task.start().unwrap();
            old_task.succeed(json!(null)).unwrap();
        } else {
            old_task.request_abort().unwrap();
        }
        let store = h.subagent_store().unwrap();
        TaskStore::new(store)
            .save(&[Save::Create(&old_task)])
            .unwrap();
        let edge = Edge {
            task: task.id(),
            task_owner: TaskOwner::Conversation(conversation),
            child: conversation,
            mode: Mode::Foreground,
        };
        store
            .commit(
                &[],
                &[Mutation::Put {
                    key: ownership_edge::edge_key(conversation),
                    value: serde_json::to_vec(&edge).unwrap(),
                }],
            )
            .unwrap();
        let parent = TaskStore::new(store).load(task.id()).unwrap().unwrap();
        let child = Task::new(
            Uuid::new_v4(),
            TaskOwner::Task(task.id()),
            Checkpoint {
                phase: "child".into(),
                data: json!(null),
            },
            4,
        )
        .unwrap();
        let child_id = child.id();
        let (ready_tx, ready_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();
        let writer = h.worker();
        let thread = std::thread::spawn(move || {
            ready_rx.recv_timeout(Duration::from_secs(10)).unwrap();
            // Model an old low-level writer that publishes an ownership change
            // and advances its owner's version in the same checked commit.
            writer
                .subagent_store()
                .unwrap()
                .commit(
                    &[Check {
                        key: crate::task_store::key(parent.task.id()),
                        expected: Expected::Version(parent.version),
                    }],
                    &[
                        Mutation::Put {
                            key: crate::task_store::key(parent.task.id()),
                            value: serde_json::to_vec(&parent.task).unwrap(),
                        },
                        Mutation::Put {
                            key: crate::task_store::key(child.id()),
                            value: serde_json::to_vec(&child).unwrap(),
                        },
                    ],
                )
                .unwrap();
            let version = TaskStore::new(writer.subagent_store().unwrap())
                .load(parent.task.id())
                .unwrap()
                .unwrap()
                .version;
            done_tx.send(version).unwrap();
        });
        let mut attempts = 0;
        let mut writer_version = 0;
        h.abort_owned_graph_impl_with(Some(conversation), None, None, false, || {
            attempts += 1;
            if attempts == 1 {
                ready_tx.send(()).unwrap();
                writer_version = done_rx.recv_timeout(Duration::from_secs(10)).unwrap();
            }
        })
        .unwrap();
        thread.join().unwrap();
        assert_eq!(
            attempts, 2,
            "changed visited task must trigger a full traversal retry"
        );
        assert_eq!(
            TaskStore::new(h.subagent_store().unwrap())
                .load(task.id())
                .unwrap()
                .unwrap()
                .version,
            writer_version,
            "retry must not rewrite an unchanged marked or terminal task"
        );
        assert!(
            TaskStore::new(h.subagent_store().unwrap())
                .load(child_id)
                .unwrap()
                .unwrap()
                .task
                .abort_requested()
        );
        drop(h);
        let reopened = Harness::open_paused(&path, options()).unwrap();
        assert!(reopened.conversation(conversation).unwrap().abort_requested);
        assert!(
            TaskStore::new(reopened.subagent_store().unwrap())
                .load(child_id)
                .unwrap()
                .unwrap()
                .task
                .abort_requested()
        );
    }
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
fn position_fork_uses_the_occurrence_commit_not_an_earlier_reference() {
    let dir = tempfile::tempdir().unwrap();
    let mut h = Harness::open_paused(dir.path().join("db"), options()).unwrap();
    let id = h
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 10,
        })
        .unwrap();
    // Reserve an earlier public observer record, then simulate it referring to
    // the later cursor. Observer payloads are not runtime authorization.
    let observer = crate::observer::Observer::new(h.owner.store(), id);
    let extra = observer.prepare_append(&serde_json::json!({})).unwrap();
    let earlier = extra.cursor.0;
    h.owner
        .store()
        .commit(&extra.checks, &extra.mutations)
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
        .remove(0);
    let mut key = b"durable/observer/".to_vec();
    key.extend_from_slice(id.as_bytes());
    key.extend_from_slice(b"/r");
    key.extend_from_slice(&earlier.to_be_bytes());
    h.owner
        .store()
        .commit(
            &[],
            &[Mutation::Put {
                key,
                value: serde_json::to_vec(&serde_json::json!({
                    "_transcript": entry.position.cursor,
                }))
                .unwrap(),
            }],
        )
        .unwrap();
    let fork = h.fork_at_position(id, entry.position).unwrap();
    assert_eq!(h.conversation(fork).unwrap().messages, vec![entry.message]);
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
fn compacted_replacement_rejects_collision_and_unproven_tail() {
    for case in 0..5 {
        let collision = case == 0;
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
        state.messages = vec![
            Message::User {
                content: "old".into(),
            },
            Message::User {
                content: "kept".into(),
            },
        ];
        h.save(&state, Expected::Version(version)).unwrap();
        let prior = h
            .view_positioned(id, std::num::NonZeroUsize::new(10).unwrap())
            .unwrap();
        let used = prior.entries[0].history.as_ref().unwrap().id;
        let other = prior.entries[1].history.clone();
        let (mut state, _) = h.load(id).unwrap();
        let transcript = crate::transcript::Transcript::new(h.owner.store(), id);
        let mut observation = crate::observer::Observer::new(h.owner.store(), id)
            .prepare_append(&serde_json::json!({}))
            .unwrap();
        let summary_id = if collision { used } else { Uuid::new_v4() };
        let change = TranscriptChange::ReplaceCompacted {
            messages: vec![
                Message::User {
                    content: "summary".into(),
                },
                Message::User {
                    content: if case == 1 { "forged" } else { "kept" }.into(),
                },
            ],
            system_end: 0,
            first_kept: 1,
            summary_id,
            commit: observation.cursor.0,
            origin: id,
            histories: vec![
                Some(MessageHistory {
                    id: summary_id,
                    commit: observation.cursor.0,
                    origin: id,
                    commit_boundary: false,
                }),
                if case == 2 {
                    other
                } else {
                    prior.entries[0].history.clone()
                },
            ],
        };
        let (batch, cursor) = transcript
            .prepare_write(&serde_json::to_value(change).unwrap(), false)
            .unwrap();
        observation
            .set_value(&serde_json::json!({"_transcript": cursor}))
            .unwrap();
        let forged_snapshot = serde_json::json!({
            "conversation_id": id,
            "_transcript": if case == 4 { prior.entries[0].position.cursor.clone() } else { cursor.clone() },
        });
        state.transcript = Some(cursor);
        state.messages.clear();
        let mut mutations = batch.mutations;
        if case != 3 {
            // Exercise projection and collision checks independently of the
            // missing-runtime-snapshot rejection tested above.
            mutations.push(Mutation::Put {
                key: format!("runtime/fork/{id}/{:020}", observation.cursor.0).into_bytes(),
                value: serde_json::to_vec(&forged_snapshot).unwrap(),
            });
        }
        mutations.push(Mutation::Put {
            key: Harness::key(id),
            value: serde_json::to_vec(&state).unwrap(),
        });
        let mut checks = batch.checks;
        checks.extend(observation.checks);
        mutations.extend(observation.mutations);
        h.owner.store().commit(&checks, &mutations).unwrap();
        if case == 3 {
            // This is an otherwise valid projection, appended through the
            // public transcript and observer APIs, without a runtime commit.
            assert!(
                h.resolved_occurrences(id, state.transcript.clone().unwrap())
                    .is_err()
            );
        }
        if collision {
            assert!(
                h.view_positioned(id, std::num::NonZeroUsize::new(10).unwrap())
                    .is_err()
            );
        }
        assert!(h.conversation(id).is_err());
        let attempted = if case == 2 {
            prior.entries[1].history.as_ref().unwrap().id
        } else {
            used
        };
        assert!(h.fork_by_entry_id(id, attempted).is_err());
    }
}

#[tokio::test]
async fn compaction_missing_original_observation_fails_closed() {
    struct Summary;
    #[async_trait::async_trait]
    impl crate::extension::Hook for Summary {
        async fn before_compact(
            &self,
            _: &crate::extension::CompactionInput,
            _: &crate::extension::Context<'_>,
        ) -> Result<crate::extension::BeforeCompact, String> {
            Ok(crate::extension::BeforeCompact::Summary("summary".into()))
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let mut h = Harness::open_paused(dir.path().join("db"), options()).unwrap();
    h.install_extension(crate::extension::Extension {
        name: "summary".into(),
        hooks: vec![Arc::new(Summary)],
        ..Default::default()
    })
    .unwrap();
    let id = h
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 10,
        })
        .unwrap();
    h.select_extensions(id, vec!["summary".into()]).unwrap();
    let (mut state, version) = h.load(id).unwrap();
    state.messages = vec![
        Message::User {
            content: "old".into(),
        },
        Message::User {
            content: "kept".into(),
        },
    ];
    h.save(&state, Expected::Version(version)).unwrap();
    let original = h
        .view_positioned(id, std::num::NonZeroUsize::new(10).unwrap())
        .unwrap()
        .entries[0]
        .history
        .clone()
        .unwrap();
    assert!(h.request_compaction(id, 0).unwrap());
    h.resume(&CancellationToken::new()).await.unwrap();
    let mut key = b"durable/observer/".to_vec();
    key.extend_from_slice(id.as_bytes());
    key.extend_from_slice(b"/r");
    key.extend_from_slice(&original.commit.to_be_bytes());
    h.owner
        .store()
        .commit(&[], &[Mutation::Delete { key }])
        .unwrap();
    assert!(h.conversation(id).is_err());
    assert!(h.fork_by_entry_id(id, original.id).is_err());
}

#[tokio::test]
async fn identical_reset_creates_new_identity_and_legacy_target_is_conservative() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let id;
    {
        let mut h = Harness::open_paused(&path, options()).unwrap();
        id = h
            .create_conversation(Config {
                model: "test".into(),
                system_prompt: "".into(),
                max_output_tokens: 10,
            })
            .unwrap();
        let legacy = h
            .create_conversation(Config {
                model: "test".into(),
                system_prompt: "".into(),
                max_output_tokens: 10,
            })
            .unwrap();
        let (mut old, old_version) = h.load(legacy).unwrap();
        old.messages = vec![
            Message::User {
                content: "old".into(),
            },
            Message::User {
                content: "target".into(),
            },
        ];
        old.occurrence_epoch_proven = false;
        h.save(&old, Expected::Version(old_version)).unwrap();
        h.request_manual_compaction(legacy, 0, None)
            .unwrap()
            .unwrap();
        let old = h.conversation(legacy).unwrap();
        let Phase::CompactionHook { preparation, .. } = old.phase else {
            panic!("missing legacy preparation")
        };
        let old_result = preparation
            .finish(CompletedGeneration {
                content: "summary".into(),
                calls: vec![],
                usage: None,
                metadata: Default::default(),
            })
            .unwrap();
        assert!(old_result.retained_target().is_none());
        let (mut state, version) = h.load(id).unwrap();
        state.messages = vec![Message::User {
            content: "handoff".into(),
        }];
        state.occurrence_epoch_proven = false;
        h.save(&state, Expected::Version(version)).unwrap();
        let before = h
            .view_positioned(id, std::num::NonZeroUsize::MIN)
            .unwrap()
            .entries[0]
            .history
            .clone()
            .unwrap()
            .id;
        h.reset(id, Some("handoff".into())).unwrap();
        let after = h
            .view_positioned(id, std::num::NonZeroUsize::MIN)
            .unwrap()
            .entries[0]
            .history
            .clone()
            .unwrap()
            .id;
        assert_ne!(before, after);
        assert!(h.conversation(id).unwrap().occurrence_epoch_proven);
    }
    let mut h = Harness::open_paused(&path, options()).unwrap();
    assert!(h.conversation(id).unwrap().occurrence_epoch_proven);
    let (mut state, version) = h.load(id).unwrap();
    state.messages.push(Message::User {
        content: "target".into(),
    });
    h.save(&state, Expected::Version(version)).unwrap();
    h.request_manual_compaction(id, 0, None).unwrap().unwrap();
    let state = h.conversation(id).unwrap();
    let Phase::CompactionHook { preparation, .. } = &state.phase else {
        panic!("missing preparation")
    };
    let result = (**preparation)
        .clone()
        .finish(CompletedGeneration {
            content: "summary".into(),
            calls: vec![],
            usage: None,
            metadata: Default::default(),
        })
        .unwrap();
    assert!(result.retained_target().is_some());
}

#[tokio::test]
async fn manual_target_rejects_forged_or_unrelated_source() {
    let dir = tempfile::tempdir().unwrap();
    let mut h = Harness::open_paused(dir.path().join("db"), options()).unwrap();
    let config = Config {
        model: "test".into(),
        system_prompt: "".into(),
        max_output_tokens: 10,
    };
    let id = h.create_conversation(config.clone()).unwrap();
    let other = h.create_conversation(config).unwrap();
    let other_cursor = h.conversation(other).unwrap().transcript.unwrap();
    let older_cursor = h.conversation(id).unwrap().transcript.unwrap();
    let (mut state, version) = h.load(id).unwrap();
    state.messages = vec![
        Message::User {
            content: "old".into(),
        },
        Message::User {
            content: "target".into(),
        },
    ];
    h.save(&state, Expected::Version(version)).unwrap();
    h.request_manual_compaction(id, 0, None).unwrap().unwrap();
    let state = h.conversation(id).unwrap();
    let Phase::CompactionHook { preparation, .. } = &state.phase else {
        panic!("missing preparation")
    };
    let result = (**preparation)
        .clone()
        .finish(CompletedGeneration {
            content: "summary".into(),
            calls: vec![],
            usage: None,
            metadata: Default::default(),
        })
        .unwrap();
    assert!(
        h.apply_manual_compaction_target(&state, &result)
            .unwrap()
            .is_some()
    );
    let encoded = serde_json::to_value(result).unwrap();
    for (field, replacement) in [
        ("id", serde_json::json!(Uuid::new_v4())),
        ("reset_id", serde_json::json!(Uuid::new_v4())),
        ("source", serde_json::to_value(other_cursor).unwrap()),
        ("source", serde_json::to_value(older_cursor).unwrap()),
    ] {
        let mut forged = encoded.clone();
        forged["preparation"]["retained_target"][field] = replacement;
        let forged: crate::compact::CompactionResult = serde_json::from_value(forged).unwrap();
        assert!(
            h.apply_manual_compaction_target(&state, &forged)
                .unwrap()
                .is_none()
        );
    }

    // A real target cannot authenticate a different summary source or cut.
    for field in ["source", "first_kept"] {
        let mut forged = encoded.clone();
        if field == "source" {
            forged["preparation"]["source"][0]["content"] = serde_json::json!("different source");
        } else {
            forged["preparation"]["first_kept"] = serde_json::json!(0);
        }
        let forged: crate::compact::CompactionResult = serde_json::from_value(forged).unwrap();
        assert!(
            h.apply_manual_compaction_target(&state, &forged)
                .unwrap()
                .is_none()
        );
    }

    // Missing target metadata uses the legacy snapshot-and-generation check;
    // it must not gain the relaxed behavior after an intervening compaction.
    let mut legacy = encoded;
    legacy["preparation"]
        .as_object_mut()
        .unwrap()
        .remove("retained_target");
    let legacy: crate::compact::CompactionResult = serde_json::from_value(legacy).unwrap();
    assert!(
        h.apply_manual_compaction_target(&state, &legacy)
            .unwrap()
            .is_some()
    );
    let mut changed = state.clone();
    changed.context_generation = Some(Uuid::new_v4());
    assert!(
        h.apply_manual_compaction_target(&changed, &legacy)
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn overlapping_manual_results_follow_retained_target_after_reopen() {
    struct Summary;
    #[async_trait::async_trait]
    impl crate::extension::Hook for Summary {
        async fn before_compact(
            &self,
            _: &crate::extension::CompactionInput,
            _: &crate::extension::Context<'_>,
        ) -> Result<crate::extension::BeforeCompact, String> {
            Ok(crate::extension::BeforeCompact::Summary(
                "first completion".into(),
            ))
        }
    }
    for case in 0..5 {
        let retain_target = case != 1 && case != 3;
        let target_text = if case >= 2 { "newer" } else { "target" };
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let (id, operation, target_id);
        {
            let mut h = Harness::open_paused(&path, options()).unwrap();
            h.install_extension(crate::extension::Extension {
                name: "summary".into(),
                hooks: vec![Arc::new(Summary)],
                ..Default::default()
            })
            .unwrap();
            id = h
                .create_conversation(Config {
                    model: "test".into(),
                    system_prompt: "".into(),
                    max_output_tokens: 10,
                })
                .unwrap();
            h.select_extensions(id, vec!["summary".into()]).unwrap();
            let (mut state, version) = h.load(id).unwrap();
            state.messages = vec![
                Message::User {
                    content: "old".repeat(100),
                },
                Message::User {
                    content: "target".into(),
                },
                Message::User {
                    content: "newer".into(),
                },
            ];
            h.save(&state, Expected::Version(version)).unwrap();
            target_id = h.view_positioned(id, std::num::NonZeroUsize::new(10).unwrap()).unwrap().entries.into_iter().find(|entry| matches!(&entry.message, Message::User { content } if content == target_text)).unwrap().history.unwrap().id;
            let messages = h.conversation(id).unwrap().messages;
            let budget = (0..1000)
                .find(|budget| {
                    crate::compact::prepare(&messages, "test", 10, *budget, None)
                        .unwrap()
                        .is_some_and(|p| p.first_kept() == 1)
                })
                .unwrap();
            operation = h
                .request_manual_compaction(id, if case >= 2 { 0 } else { budget }, None)
                .unwrap()
                .unwrap();
            let (mut state, version) = h.load(id).unwrap();
            let Phase::CompactionHook { preparation, .. } = state.phase.clone() else {
                panic!("missing preparation")
            };
            let later = preparation
                .finish(CompletedGeneration {
                    content: "later completion".into(),
                    calls: vec![],
                    usage: None,
                    metadata: Default::default(),
                })
                .unwrap();
            // Model an independently retained result while the existing public
            // API remains idle-only; the competing compaction below is real.
            state.phase = Phase::Idle;
            state.active_manual_compaction = None;
            h.save(&state, Expected::Version(version)).unwrap();
            assert!(
                h.request_compaction(id, if case == 1 { 0 } else { budget })
                    .unwrap()
            );
            h.resume(&CancellationToken::new()).await.unwrap();
            let (mut state, version) = h.load(id).unwrap();
            state.active_manual_compaction = Some(operation);
            if case == 3 {
                state.phase = Phase::CompactionReady {
                    preparation: Box::new(
                        crate::compact::prepare(&state.messages, "test", 10, 0, None)
                            .unwrap()
                            .unwrap(),
                    ),
                    restarts: 0,
                };
                h.save(&state, Expected::Version(version)).unwrap();
                h.reset(id, Some("ordered reset".into())).unwrap();
                let loaded = h.load(id).unwrap();
                state = loaded.0;
                let version = loaded.1;
                let resets_before =
                    Some(state.pending_resets.iter().map(|reset| reset.id).collect());
                state.phase = Phase::CompactionPlacement {
                    result: Box::new(later),
                    resets_before,
                };
                h.save(&state, Expected::Version(version)).unwrap();
            } else {
                state.phase = Phase::CompactionPlacement {
                    result: Box::new(later),
                    resets_before: Some(vec![]),
                };
                h.save(&state, Expected::Version(version)).unwrap();
                if case == 4 {
                    h.reset(id, Some("ordered reset".into())).unwrap();
                }
            }
        }
        let mut h = Harness::open_paused(&path, options()).unwrap();
        h.resume(&CancellationToken::new()).await.unwrap();
        let c = h.conversation(id).unwrap();
        let status = &c
            .manual_compactions
            .iter()
            .find(|item| item.id == operation)
            .unwrap()
            .status;
        assert_eq!(
            *status,
            if retain_target {
                ManualCompactionStatus::Applied
            } else {
                ManualCompactionStatus::Stale
            }
        );
        if case == 4 {
            assert_eq!(
                c.messages,
                vec![Message::User {
                    content: "ordered reset".into()
                }]
            );
        } else if retain_target {
            assert!(c.messages.iter().any(
                |m| matches!(m, Message::User { content } if content.contains("later completion"))
            ));
            let target = h.view_positioned(id, std::num::NonZeroUsize::new(10).unwrap()).unwrap().entries.into_iter().find(|entry| matches!(&entry.message, Message::User { content } if content == target_text)).unwrap().history.unwrap();
            assert_eq!(target.id, target_id);
        } else {
            assert!(!c.messages.iter().any(
                |m| matches!(m, Message::User { content } if content.contains("later completion"))
            ));
        }
    }
}

#[tokio::test]
async fn repeated_compaction_retains_original_provenance() {
    struct Summary;
    #[async_trait::async_trait]
    impl crate::extension::Hook for Summary {
        async fn before_compact(
            &self,
            _: &crate::extension::CompactionInput,
            _: &crate::extension::Context<'_>,
        ) -> Result<crate::extension::BeforeCompact, String> {
            Ok(crate::extension::BeforeCompact::Summary("summary".into()))
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let mut h = Harness::open_paused(dir.path().join("db"), options()).unwrap();
    h.install_extension(crate::extension::Extension {
        name: "summary".into(),
        hooks: vec![Arc::new(Summary)],
        ..Default::default()
    })
    .unwrap();
    let id = h
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 10,
        })
        .unwrap();
    h.select_extensions(id, vec!["summary".into()]).unwrap();
    let (mut state, version) = h.load(id).unwrap();
    state.messages = vec![
        Message::User {
            content: "old".repeat(100),
        },
        Message::User {
            content: "kept".into(),
        },
    ];
    h.save(&state, Expected::Version(version)).unwrap();
    let history = h
        .view_positioned(id, std::num::NonZeroUsize::new(10).unwrap())
        .unwrap()
        .entries[0]
        .history
        .clone()
        .unwrap();
    assert!(h.request_compaction(id, 0).unwrap());
    h.resume(&CancellationToken::new()).await.unwrap();
    let (mut state, version) = h.load(id).unwrap();
    state.messages.push(Message::User {
        content: "new".into(),
    });
    h.save(&state, Expected::Version(version)).unwrap();
    let state = h.conversation(id).unwrap();
    let index = state
        .messages
        .iter()
        .position(|m| matches!(m, Message::User { content } if content == "kept"))
        .unwrap();
    let budget = (0..1000)
        .find(|budget| {
            crate::compact::prepare(&state.messages, "test", 10, *budget, None)
                .unwrap()
                .is_some_and(|p| {
                    p.hook_input(crate::extension::CompactionReason::Manual)
                        .first_kept
                        == index
                })
        })
        .unwrap();
    assert!(h.request_compaction(id, budget).unwrap());
    h.resume(&CancellationToken::new()).await.unwrap();
    let retained = h
        .view_positioned(id, std::num::NonZeroUsize::new(10).unwrap())
        .unwrap()
        .entries
        .into_iter()
        .find(|entry| matches!(&entry.message, Message::User { content } if content == "kept"))
        .unwrap()
        .history
        .unwrap();
    assert_eq!(retained, history);
    let fork = h.fork_by_entry_id(id, history.id).unwrap();
    assert!(
        h.conversation(fork)
            .unwrap()
            .messages
            .iter()
            .any(|m| matches!(m, Message::User { content } if content == "kept"))
    );
}

#[tokio::test]
async fn compaction_keeps_legacy_occurrences_unidentified() {
    struct Summary;
    #[async_trait::async_trait]
    impl crate::extension::Hook for Summary {
        async fn before_compact(
            &self,
            _: &crate::extension::CompactionInput,
            _: &crate::extension::Context<'_>,
        ) -> Result<crate::extension::BeforeCompact, String> {
            Ok(crate::extension::BeforeCompact::Summary("summary".into()))
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let id;
    {
        let mut h = Harness::open_paused(&path, options()).unwrap();
        h.install_extension(crate::extension::Extension {
            name: "summary".into(),
            hooks: vec![Arc::new(Summary)],
            ..Default::default()
        })
        .unwrap();
        id = h
            .create_conversation(Config {
                model: "test".into(),
                system_prompt: "".into(),
                max_output_tokens: 10,
            })
            .unwrap();
        h.select_extensions(id, vec!["summary".into()]).unwrap();
        let (mut state, _) = h.load(id).unwrap();
        let transcript = crate::transcript::Transcript::new(h.owner.store(), id);
        let change = TranscriptChange::Replace(vec![
            Message::User {
                content: "old".into(),
            },
            Message::User {
                content: "kept".into(),
            },
        ]);
        let (batch, cursor) = transcript
            .prepare_write(&serde_json::to_value(change).unwrap(), false)
            .unwrap();
        state.transcript = Some(cursor);
        state.messages.clear();
        let mut mutations = batch.mutations;
        mutations.push(Mutation::Put {
            key: Harness::key(id),
            value: serde_json::to_vec(&state).unwrap(),
        });
        h.owner.store().commit(&batch.checks, &mutations).unwrap();
        assert!(h.request_compaction(id, 0).unwrap());
        h.resume(&CancellationToken::new()).await.unwrap();
    }
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let entries = h
        .view_positioned(id, std::num::NonZeroUsize::new(10).unwrap())
        .unwrap()
        .entries;
    let kept = entries
        .iter()
        .find(|entry| matches!(&entry.message, Message::User { content } if content == "kept"))
        .unwrap();
    assert!(kept.history.is_none());
    assert!(h.fork_at_position(id, kept.position.clone()).is_err());
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
fn current_fork_returns_conflict_when_parent_keeps_changing() {
    let dir = tempfile::tempdir().unwrap();
    let mut h = Harness::open_paused(dir.path().join("db"), options()).unwrap();
    let parent = h
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: String::new(),
            max_output_tokens: 10,
        })
        .unwrap();
    let mut writer = h.worker();
    let mut calls = 0;
    let result = h.fork_with_load_hook(parent, || {
        calls += 1;
        let (state, version) = writer.load(parent).unwrap();
        writer.save(&state, Expected::Version(version)).unwrap();
    });
    assert!(matches!(
        result,
        Err(Error::Storage(durable_store::Error::Conflict { .. }))
    ));
    assert_eq!(calls, FORK_DOCUMENT_ATTEMPTS);
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
async fn repeated_background_abort_preserves_running_cleanup_reservation() {
    use crate::task::{Checkpoint, Task, TaskOwner};
    use crate::task_store::{Save, TaskStore};
    let dir = tempfile::tempdir().unwrap();
    let mut h = Harness::open_paused(dir.path().join("db"), options()).unwrap();
    let id = h
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
    TaskStore::new(h.owner.store())
        .save(&[Save::Create(&task)])
        .unwrap();
    h.request_abort_with_background(id).unwrap();
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let release_rx = std::sync::Mutex::new(release_rx);
    let mut registry = crate::task_scheduler::Registry::default();
    registry.register_with_abort(
        "job",
        true,
        |_: &Task| {
            Ok(crate::task_scheduler::Step::Succeed(
                serde_json::Value::Null,
            ))
        },
        move |_: &Task| {
            started_tx.send(()).unwrap();
            release_rx
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(5))
                .unwrap();
            Ok(crate::task_scheduler::Step::Succeed(
                serde_json::Value::Null,
            ))
        },
    );
    let driver = h.start_tasks(registry).unwrap();
    started_rx.recv_timeout(Duration::from_secs(5)).unwrap();
    h.request_abort_with_background(id).unwrap();
    release_tx.send(()).unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let current = driver.get(task.id()).unwrap().unwrap();
        if current.is_terminal() {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "cleanup did not finish"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    driver.shutdown().unwrap();
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
    finish_failure(&mut stale, "failure".into(), FailureCause::ModelError);
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
    finish_failure(&mut state, "failure".into(), FailureCause::ModelError);
    h.save(&state, Expected::Version(version)).unwrap();
    assert!(matches!(
        h.conversation(id).unwrap().phase,
        Phase::ModelReady { turns: 0 }
    ));
    assert!(matches!(
        h.submit_continue(id, None),
        Err(Error::Admission(AdmissionError::Busy))
    ));
    assert!(matches!(
        h.submit_with_options(
            id,
            None,
            "rejected".into(),
            AdmissionOptions {
                reject_when_busy: true,
                ..Default::default()
            }
        ),
        Err(Error::Admission(AdmissionError::Busy))
    ));
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
        Status::Running
    );
    assert!(matches!(
        h.submit_continue(id, None),
        Err(Error::Admission(AdmissionError::Busy))
    ));
    assert!(h.conversation(id).unwrap().run_failure.is_some());
    h.resume_conversation(id, &CancellationToken::new())
        .await
        .unwrap();
    assert!(h.conversation(id).unwrap().run_failure.is_some());
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

#[test]
fn withdrawn_auto_continuation_does_not_fence_future_continue() {
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
    h.set_after_failed_run(id, AfterFailedRun::Continue)
        .unwrap();
    h.submit(id, None, "active".into()).unwrap();
    let queued = h.submit(id, None, "queued".into()).unwrap();
    h.reset(id, Some("ordered reset".into())).unwrap();
    let (mut state, version) = h.load(id).unwrap();
    finish_failure(&mut state, "failure".into(), FailureCause::ModelError);
    h.save(&state, Expected::Version(version)).unwrap();
    assert!(matches!(
        h.submit_continue(id, None),
        Err(Error::Admission(AdmissionError::Busy))
    ));
    assert_eq!(
        h.withdraw_submission(id, queued).unwrap(),
        WithdrawResult::Aborted
    );
    drop(h);
    let mut h = Harness::open_paused(&path, options()).unwrap();
    h.submit_continue(id, None).unwrap();
    assert!(h.conversation(id).unwrap().run_failure.is_none());
}

#[cfg(unix)]
#[test]
fn failure_marker_kill_child() {
    use std::io::Write;
    let Ok(path) = std::env::var("PI_FAILURE_MARKER_KILL_DB") else {
        return;
    };
    let mut h = Harness::open_paused(path, options()).unwrap();
    let id = h
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 10,
        })
        .unwrap();
    h.set_after_failed_run(id, AfterFailedRun::Continue)
        .unwrap();
    h.submit(id, None, "active".into()).unwrap();
    let queued = h.submit(id, None, "queued".into()).unwrap();
    let (mut state, version) = h.load(id).unwrap();
    finish_failure(&mut state, "failure".into(), FailureCause::ModelError);
    h.save(&state, Expected::Version(version)).unwrap();
    println!("FAILURE_MARKER_COMMITTED {id} {queued}");
    std::io::stdout().flush().unwrap();
    loop {
        std::thread::park();
    }
}

#[cfg(unix)]
#[tokio::test]
async fn pending_auto_continuation_fences_continue_after_process_kill() {
    use std::io::{BufRead, BufReader};
    use std::process::{Command, Stdio};
    use std::time::Duration;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "runtime::runtime_cas_tests::failure_marker_kill_child",
            "--nocapture",
        ])
        .env("PI_FAILURE_MARKER_KILL_DB", &path)
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let stdout = child.stdout.take().unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines().map_while(Result::ok) {
            if let Some((_, rest)) = line.split_once("FAILURE_MARKER_COMMITTED ") {
                let _ = tx.send(rest.to_string());
                break;
            }
        }
    });
    let marker = rx.recv_timeout(Duration::from_secs(20));
    child.kill().unwrap();
    child.wait().unwrap();
    let marker = marker.expect("child did not commit failure");
    let mut ids = marker
        .split_whitespace()
        .map(|v| Uuid::parse_str(v).unwrap());
    let id = ids.next().unwrap();
    let queued = ids.next().unwrap();
    let mut h = Harness::open_paused(&path, options()).unwrap();
    assert_eq!(
        h.withdraw_submission(id, queued).unwrap(),
        WithdrawResult::AlreadyPlaced
    );
    assert!(matches!(
        h.submit_continue(id, None),
        Err(Error::Admission(AdmissionError::Busy))
    ));
    assert!(h.conversation(id).unwrap().run_failure.is_some());
    h.resume_conversation(id, &CancellationToken::new())
        .await
        .unwrap();
    let c = h.conversation(id).unwrap();
    assert!(matches!(
        c.submissions
            .iter()
            .find(|s| s.id == queued)
            .unwrap()
            .status,
        Status::Done { .. }
    ));
    assert!(c.run_failure.is_some());
}

#[tokio::test]
async fn abort_of_pending_auto_continuation_finishes_before_new_continue() {
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
    h.set_after_failed_run(id, AfterFailedRun::Continue)
        .unwrap();
    h.submit(id, None, "active".into()).unwrap();
    let queued = h.submit(id, None, "queued".into()).unwrap();
    h.reset(id, Some("ordered reset".into())).unwrap();
    let (mut state, version) = h.load(id).unwrap();
    finish_failure(&mut state, "failure".into(), FailureCause::ModelError);
    h.save(&state, Expected::Version(version)).unwrap();
    h.request_abort(id).unwrap();
    assert!(matches!(
        h.submit_continue(id, None),
        Err(Error::Admission(AdmissionError::Busy))
    ));
    assert_eq!(
        h.conversation(id)
            .unwrap()
            .submissions
            .iter()
            .find(|s| s.id == queued)
            .unwrap()
            .status,
        Status::Unanswered {
            reason: "aborted".into()
        }
    );
    drop(h);
    let mut h = Harness::open_paused(&path, options()).unwrap();
    assert!(matches!(
        h.submit_continue(id, None),
        Err(Error::Admission(AdmissionError::Busy))
    ));
    h.resume_conversation(id, &CancellationToken::new())
        .await
        .unwrap();
    h.submit_continue(id, None).unwrap();
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
    finish_failure(&mut stale, "failure".into(), FailureCause::ModelError);
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
fn managed_anchor_creation_waits_for_abort_graph_gate() {
    use std::sync::mpsc;
    use std::time::Duration;

    for existing in 0..3 {
        let dir = tempfile::tempdir().unwrap();
        let mut h = Harness::open_paused(dir.path().join("db"), options()).unwrap();
        let config = Config {
            model: "test".into(),
            system_prompt: String::new(),
            max_output_tokens: 10,
        };
        let parent = h.create_conversation(config.clone()).unwrap();
        if existing > 0 {
            h.ensure_subagent(parent, "worker", config.clone()).unwrap();
        }
        if existing == 2 {
            h.anchor_subagent(parent, "worker").unwrap();
        }
        let owner = h.owner.clone();
        let gate = owner.task_gate().lock().unwrap();
        let mut worker = h.worker();
        let (started_tx, started_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();
        let thread = std::thread::spawn(move || {
            started_tx.send(()).unwrap();
            let result = worker.ensure_managed_subagent(parent, "worker", config, None);
            done_tx.send(result).unwrap();
        });
        started_rx.recv().unwrap();
        assert!(matches!(
            done_rx.recv_timeout(Duration::from_millis(50)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ));
        // Model the parent mark committed while abort holds the gate. Admission
        // must re-read it after acquiring the gate rather than create an anchor.
        let (mut state, version) = h.load(parent).unwrap();
        state.abort_requested = true;
        h.save(&state, Expected::Version(version)).unwrap();
        drop(gate);
        assert!(
            done_rx
                .recv_timeout(Duration::from_secs(2))
                .unwrap()
                .is_err()
        );
        thread.join().unwrap();
        assert_eq!(
            owner
                .store()
                .list(format!("runtime/subagent-anchor/{parent}/").as_bytes())
                .unwrap()
                .len(),
            usize::from(existing == 2)
        );
        if existing == 0 {
            assert!(h.subagent(parent, "worker").unwrap().is_none());
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

#[test]
fn reporter_generation_fences_prepared_admission_and_survives_reopen() {
    use crate::subagent::generation_check;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let config = Config {
        model: "test".into(),
        system_prompt: String::new(),
        max_output_tokens: 10,
    };
    let parent = h.create_conversation(config.clone()).unwrap();
    let link = h.ensure_subagent(parent, "worker", config.clone()).unwrap();
    let (generation, check) = generation_check(h.owner.store(), parent).unwrap();
    let (mut child, version) = h.load(link.child).unwrap();
    child.admit_initial_input(Uuid::new_v4(), "stale".into());
    let mut batch = h.prepare_save(&child, Expected::Version(version)).unwrap();
    batch.checks.push(check);
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let other = barrier.clone();
    let mut abort = h.worker();
    let thread = std::thread::spawn(move || {
        other.wait();
        abort.request_abort_with_background(parent).unwrap();
    });
    barrier.wait();
    thread.join().unwrap();
    assert_eq!(h.load(link.child).unwrap().1, version);
    assert!(matches!(
        h.commit_conversation_save(batch),
        Err(Error::Storage(durable_store::Error::Conflict { .. }))
    ));
    assert!(
        h.submit_reporter_input(
            link.child,
            "stale".into(),
            "stale".into(),
            InputMode::FollowUp,
            parent,
            generation
        )
        .is_err()
    );
    drop(h);
    let h = Harness::open_paused(&path, options()).unwrap();
    assert_eq!(
        generation_check(h.owner.store(), parent).unwrap().0,
        generation + 1
    );
    assert!(h.conversation(link.child).unwrap().submissions.is_empty());
    assert!(h.pending_reporters().unwrap().is_empty());
}

#[test]
fn reporter_operation_ids_are_unique_per_parent() {
    use crate::subagent::ReportedInput;
    let dir = tempfile::tempdir().unwrap();
    let mut h = Harness::open_paused(dir.path().join("db"), options()).unwrap();
    let config = Config {
        model: "test".into(),
        system_prompt: String::new(),
        max_output_tokens: 10,
    };
    let parent = h.create_conversation(config.clone()).unwrap();
    let input = || ReportedInput {
        history: false,
        request_id: "same".into(),
        content: "hello".into(),
        mode: InputMode::FollowUp,
    };
    h.submit_subagent_reported(parent, "one", config.clone(), input())
        .unwrap();
    assert!(
        h.submit_subagent_reported(parent, "two", config, input())
            .is_err()
    );
}

#[test]
fn legacy_child_key_does_not_block_nested_parent_operation() {
    use crate::subagent::ReportedInput;
    let dir = tempfile::tempdir().unwrap();
    let mut h = Harness::open_paused(dir.path().join("db"), options()).unwrap();
    let config = Config {
        model: "test".into(),
        system_prompt: String::new(),
        max_output_tokens: 10,
    };
    let parent = h.create_conversation(config.clone()).unwrap();
    let input = || ReportedInput {
        history: false,
        request_id: "same".into(),
        content: "hello".into(),
        mode: InputMode::FollowUp,
    };
    let (link, first) = h
        .submit_subagent_reported(parent, "one", config.clone(), input())
        .unwrap();
    let store = h.owner.store();
    let records = store.list(b"runtime/subagent-report-v2/").unwrap();
    let (key, entry) = &records[0];
    let legacy = format!("runtime/subagent-report/{}/same", link.child).into_bytes();
    store
        .commit(
            &[
                Check {
                    key: key.clone(),
                    expected: Expected::Version(entry.version),
                },
                Check {
                    key: legacy.clone(),
                    expected: Expected::Missing,
                },
            ],
            &[
                Mutation::Delete { key: key.clone() },
                Mutation::Put {
                    key: legacy,
                    value: entry.value.clone(),
                },
            ],
        )
        .unwrap();
    assert_eq!(
        h.submit_subagent_reported(parent, "one", config.clone(), input())
            .unwrap()
            .1,
        first
    );
    assert!(
        h.submit_subagent_reported(parent, "other", config.clone(), input())
            .is_err()
    );
    let (nested, _) = h
        .submit_subagent_reported(link.child, "nested", config, input())
        .unwrap();
    assert_eq!(h.conversation(nested.child).unwrap().submissions.len(), 1);
    assert_eq!(h.pending_reporters().unwrap().len(), 2);
}

#[test]
fn concurrent_reporter_ids_with_different_names_have_one_winner() {
    use crate::subagent::ReportedInput;
    let dir = tempfile::tempdir().unwrap();
    let mut h = Harness::open_paused(dir.path().join("db"), options()).unwrap();
    let config = Config {
        model: "test".into(),
        system_prompt: String::new(),
        max_output_tokens: 10,
    };
    let parent = h.create_conversation(config.clone()).unwrap();
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let threads: Vec<_> = ["one", "two"]
        .into_iter()
        .map(|name| {
            let mut worker = h.worker();
            let barrier = barrier.clone();
            let config = config.clone();
            std::thread::spawn(move || {
                barrier.wait();
                worker.submit_subagent_reported(
                    parent,
                    name,
                    config,
                    ReportedInput {
                        history: false,
                        request_id: "same".into(),
                        content: "hello".into(),
                        mode: InputMode::FollowUp,
                    },
                )
            })
        })
        .collect();
    let results: Vec<_> = threads.into_iter().map(|t| t.join().unwrap()).collect();
    assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
    assert_eq!(h.pending_reporters().unwrap().len(), 1);
    let admitted: usize = ["one", "two"]
        .into_iter()
        .filter_map(|n| h.subagent(parent, n).unwrap())
        .map(|l| h.conversation(l.child).unwrap().submissions.len())
        .sum();
    assert_eq!(admitted, 1);
}

#[tokio::test]
async fn stale_reporter_retires_on_reopen_and_later_requests_work() {
    use crate::subagent::{ReportedInput, generation_check};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let config = Config {
        model: "test".into(),
        system_prompt: String::new(),
        max_output_tokens: 10,
    };
    let parent = h.create_conversation(config.clone()).unwrap();
    let input = |id: &str| ReportedInput {
        history: false,
        request_id: id.into(),
        content: "hello".into(),
        mode: InputMode::FollowUp,
    };
    let (link, _) = h
        .submit_subagent_reported(parent, "worker", config.clone(), input("old"))
        .unwrap();
    let pending = h.pending_reporters().unwrap();
    assert_eq!(pending.len(), 1);
    h.request_abort(parent).unwrap();
    assert_eq!(generation_check(h.owner.store(), parent).unwrap().0, 0);
    h.request_abort_with_background(parent).unwrap();
    assert!(h.reporter_child(&pending[0].1).unwrap().is_none());
    drop(h);
    let mut h = Harness::open_paused(&path, options()).unwrap();
    assert!(h.pending_reporters().unwrap().is_empty());
    h.resume_conversation(parent, &CancellationToken::new())
        .await
        .unwrap();
    h.resume_conversation(link.child, &CancellationToken::new())
        .await
        .unwrap();
    assert!(
        h.submit_subagent_reported(parent, "worker", config.clone(), input("old"))
            .is_err()
    );
    let (_, new_id) = h
        .submit_subagent_reported(parent, "worker", config, input("new"))
        .unwrap();
    assert!(
        h.conversation(link.child)
            .unwrap()
            .submissions
            .iter()
            .any(|s| s.id == new_id)
    );
    assert_eq!(h.pending_reporters().unwrap().len(), 1);
}

struct LegacyHandoffTool;
#[async_trait::async_trait]
impl crate::keyed_tool::KeyedTool for LegacyHandoffTool {
    fn spec(&self) -> pi_rs_core::ToolSpec {
        pi_rs_core::ToolSpec {
            name: "legacy-handoff".into(),
            description: String::new(),
            parameters: serde_json::json!({"type": "object"}),
        }
    }
    async fn invoke(
        &self,
        _: serde_json::Value,
        _: pi_rs_core::ToolContext,
        _: Option<&pi_rs_core::ExtensionState>,
        _: crate::keyed_tool::KeyedHost,
    ) -> pi_rs_core::ToolReply {
        unreachable!()
    }
    async fn invoke_controlled(
        &self,
        _: serde_json::Value,
        _: pi_rs_core::ToolContext,
        _: Option<&pi_rs_core::ExtensionState>,
        _: crate::keyed_tool::KeyedHost,
    ) -> (pi_rs_core::ToolReply, crate::reset::ToolControl) {
        (
            pi_rs_core::Outcome::Succeeded("ok".into()).into(),
            crate::reset::ToolControl {
                add_tools: Vec::new(),
                handoff: Some("new context".into()),
                terminate: true,
            },
        )
    }
}

#[tokio::test]
async fn resumed_legacy_round_records_handoff_without_termination_slots() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let tool = Arc::new(crate::keyed_tool::KeyedToolAdapter::new(LegacyHandoffTool));
    let opts = || Options {
        tools: vec![tool.register(ReplayPolicy::Safe)],
        ..options()
    };
    let id;
    {
        let mut h = Harness::open_paused(&path, opts()).unwrap();
        id = h
            .create_conversation(Config {
                model: "test".into(),
                system_prompt: String::new(),
                max_output_tokens: 10,
            })
            .unwrap();
        h.submit(id, None, "old".into()).unwrap();
        let (mut c, version) = h.load(id).unwrap();
        let calls = vec![ToolCall {
            id: "call".into(),
            name: "legacy-handoff".into(),
            arguments: "{}".into(),
        }];
        c.messages.push(Message::Assistant {
            content: "answer".into(),
            calls: calls.clone(),
            replay: None,
        });
        c.phase = Phase::Tools {
            turns: 1,
            calls,
            index: 0,
        };
        // These are the serde defaults for a round saved before either field existed.
        c.round_terminates.clear();
        c.round_handoffs.clear();
        h.save(&c, Expected::Version(version)).unwrap();
    }
    let mut h = Harness::open_paused(&path, opts()).unwrap();
    h.resume_conversation(id, &CancellationToken::new())
        .await
        .unwrap();
    let c = h.conversation(id).unwrap();
    assert_eq!(c.resets.len(), 1);
    assert_eq!(c.resets[0].handoff.as_deref(), Some("new context"));
    assert!(matches!(&c.submissions[0].status, Status::Done { answer } if answer == "answer"));
}

#[test]
fn withdrawn_write_retry_ignores_replacement_kind_and_legacy_json_defaults() {
    let dir = tempfile::tempdir().unwrap();
    let mut owner = Harness::open_paused(dir.path().join("db"), options()).unwrap();
    let id = owner
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: String::new(),
            max_output_tokens: 10,
        })
        .unwrap();
    owner.submit(id, None, "active".into()).unwrap();
    owner.reset(id, None).unwrap();
    let write = owner
        .submit_write(id, Some("key".into()), "app.note".into(), None)
        .unwrap();
    assert_eq!(
        owner.withdraw_submission(id, write).unwrap(),
        WithdrawResult::Aborted
    );
    for kind in ["", "pi.reserved"] {
        assert_eq!(
            owner
                .submit_write(id, Some("key".into()), kind.into(), None)
                .unwrap(),
            write
        );
        assert!(
            owner
                .submit_write(id, Some("other".into()), kind.into(), None)
                .is_err()
        );
    }
    let current = owner.conversation(id).unwrap();
    let mut legacy = serde_json::to_value(&current.application_writes[0]).unwrap();
    legacy.as_object_mut().unwrap().remove("withdrawn");
    let restored: ApplicationWrite = serde_json::from_value(legacy).unwrap();
    assert!(!restored.withdrawn);
    assert!(!restored.placed);
    let roundtrip: ApplicationWrite =
        serde_json::from_value(serde_json::to_value(&current.application_writes[0]).unwrap())
            .unwrap();
    assert!(roundtrip.withdrawn && !roundtrip.placed);
}

#[test]
fn withdrawal_of_new_write_survives_stale_worker_placing_its_reset() {
    let dir = tempfile::tempdir().unwrap();
    let mut owner = Harness::open_paused(dir.path().join("db"), options()).unwrap();
    let id = owner
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: String::new(),
            max_output_tokens: 10,
        })
        .unwrap();
    owner.submit(id, None, "active".into()).unwrap();
    let reset = owner.reset(id, None).unwrap();
    let mut worker = owner.worker();
    let (mut stale, version) = worker.load(id).unwrap();
    let marker = stale.pending_resets.remove(0);
    place_reset(&mut stale, marker);
    let withdrawn = owner
        .submit_write(id, Some("withdrawn".into()), "app.note".into(), None)
        .unwrap();
    let kept = owner
        .submit_write(id, Some("kept".into()), "app.note".into(), None)
        .unwrap();
    assert_eq!(
        owner.withdraw_submission(id, withdrawn).unwrap(),
        WithdrawResult::Aborted
    );
    worker.save(&stale, Expected::Version(version)).unwrap();
    let state = owner.conversation(id).unwrap();
    assert!(state.resets.iter().any(|r| r.id == reset));
    let removed = state
        .application_writes
        .iter()
        .find(|w| w.id == withdrawn)
        .unwrap();
    let placed = state
        .application_writes
        .iter()
        .find(|w| w.id == kept)
        .unwrap();
    assert!(removed.withdrawn && !removed.placed);
    assert!(!placed.withdrawn && placed.placed);
}

fn atomic_failure_replacement_fixture() -> (tempfile::TempDir, Harness, Uuid, Uuid) {
    let dir = tempfile::tempdir().unwrap();
    let mut h = Harness::open_paused(dir.path().join("db"), options()).unwrap();
    let id = h
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 10,
        })
        .unwrap();
    h.set_after_failed_run(id, AfterFailedRun::Continue)
        .unwrap();
    h.submit(id, None, "failed".into()).unwrap();
    let replacement = h.submit(id, None, "replacement".into()).unwrap();
    let (mut state, version) = h.load(id).unwrap();
    finish_failure(&mut state, "failure".into(), FailureCause::ModelError);
    h.save(&state, Expected::Version(version)).unwrap();
    (dir, h, id, replacement)
}

#[test]
fn failed_run_replacement_is_placed_at_failure_commit() {
    let (_dir, mut h, id, replacement) = atomic_failure_replacement_fixture();
    assert_eq!(
        h.withdraw_submission(id, replacement).unwrap(),
        WithdrawResult::AlreadyPlaced
    );
}

#[tokio::test]
async fn abort_after_failure_settlement_aborts_replacement_run() {
    let (_dir, mut h, id, replacement) = atomic_failure_replacement_fixture();
    h.request_abort(id).unwrap();
    h.resume_conversation(id, &CancellationToken::new())
        .await
        .unwrap();
    let c = h.conversation(id).unwrap();
    let failure = c.run_failure.unwrap();
    assert_eq!(failure.reason, "aborted");
    assert_eq!(failure.inputs, vec![replacement]);
}

#[test]
fn later_steer_does_not_join_initial_replacement_inputs() {
    let (_dir, mut h, id, replacement) = atomic_failure_replacement_fixture();
    let steer = h
        .submit_with_mode(id, None, "later steer".into(), InputMode::Steer)
        .unwrap();
    let c = h.conversation(id).unwrap();
    assert_eq!(
        c.submissions
            .iter()
            .find(|s| s.id == replacement)
            .unwrap()
            .status,
        Status::Running
    );
    assert_eq!(
        c.submissions.iter().find(|s| s.id == steer).unwrap().status,
        Status::Queued
    );
}

#[tokio::test]
async fn failed_run_with_two_block_resets_preserves_order_across_reopen() {
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
    h.set_after_failed_run(id, AfterFailedRun::Continue)
        .unwrap();
    h.submit(id, None, "active".into()).unwrap();
    let queued = h.submit(id, None, "queued".into()).unwrap();
    let first = vec![serde_json::json!({"type":"text", "text":"first"})];
    let second = vec![serde_json::json!({"type":"text", "text":"second"})];
    let (r1, ids1) = h
        .reset_with_blocks(id, Some("one".into()), vec![first.clone()])
        .unwrap();
    let (r2, ids2) = h
        .reset_with_blocks(id, Some("two".into()), vec![second.clone()])
        .unwrap();
    let (mut state, version) = h.load(id).unwrap();
    finish_failure(&mut state, "failure".into(), FailureCause::ModelError);
    h.save(&state, Expected::Version(version)).unwrap();
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
    drop(h);
    let mut h = Harness::open_paused(&path, options()).unwrap();
    // The test model has no block adapter; both resets should be committed
    // before dispatch rejects that unsupported configuration.
    assert!(
        h.resume_conversation(id, &CancellationToken::new())
            .await
            .is_err()
    );
    drop(h);
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let c = h.conversation(id).unwrap();
    assert_eq!(
        c.resets.iter().map(|r| r.id).collect::<Vec<_>>(),
        vec![r1, r2]
    );
    assert_eq!(c.content_blocks[&1].message_id, ids2[0]);
    assert_eq!(c.content_blocks[&1].blocks, second);
    let old = h.fork_by_entry_id(id, ids1[0]).unwrap();
    assert_eq!(
        h.conversation(old).unwrap().content_blocks[&1].blocks,
        first
    );
}

#[test]
fn atomic_failure_boundary_respects_per_mode_queue_limits() {
    let dir = tempfile::tempdir().unwrap();
    let mut h = Harness::open_paused(dir.path().join("db"), options()).unwrap();
    let id = h
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 10,
        })
        .unwrap();
    h.set_after_failed_run(id, AfterFailedRun::Continue)
        .unwrap();
    h.set_queue_modes(
        id,
        QueueModes {
            steering_mode: QueueMode::OneAtATime,
            follow_up_mode: QueueMode::OneAtATime,
        },
    )
    .unwrap();
    h.submit(id, None, "active".into()).unwrap();
    let steer1 = h
        .submit_with_mode(id, None, "steer1".into(), InputMode::Steer)
        .unwrap();
    let steer2 = h
        .submit_with_mode(id, None, "steer2".into(), InputMode::Steer)
        .unwrap();
    let follow1 = h.submit(id, None, "follow1".into()).unwrap();
    let follow2 = h.submit(id, None, "follow2".into()).unwrap();
    let (mut state, version) = h.load(id).unwrap();
    finish_failure(&mut state, "failure".into(), FailureCause::ModelError);
    h.save(&state, Expected::Version(version)).unwrap();
    let c = h.conversation(id).unwrap();
    for (sid, status) in [
        (steer1, Status::Running),
        (steer2, Status::Queued),
        (follow1, Status::Running),
        (follow2, Status::Queued),
    ] {
        assert_eq!(
            c.submissions.iter().find(|s| s.id == sid).unwrap().status,
            status
        );
    }
}

#[test]
fn atomic_failure_boundary_retries_with_latest_inbox_and_modes() {
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
        .set_after_failed_run(id, AfterFailedRun::Continue)
        .unwrap();
    owner.submit(id, None, "active".into()).unwrap();
    let first = owner.submit(id, None, "first".into()).unwrap();
    let mut worker = owner.worker();
    let (mut stale, version) = worker.load(id).unwrap();
    finish_failure(&mut stale, "failure".into(), FailureCause::ModelError);
    let second = owner.submit(id, None, "second".into()).unwrap();
    owner
        .set_queue_modes(
            id,
            QueueModes {
                steering_mode: QueueMode::All,
                follow_up_mode: QueueMode::All,
            },
        )
        .unwrap();
    worker.save(&stale, Expected::Version(version)).unwrap();
    let c = owner.conversation(id).unwrap();
    for sid in [first, second] {
        assert_eq!(
            c.submissions.iter().find(|s| s.id == sid).unwrap().status,
            Status::Running
        );
    }
    assert_eq!(c.run_failure.unwrap().reason, "model_error");
}

#[test]
fn historical_fork_rejects_anchor_rewritten_in_another_commit() {
    for mode in 0..3 {
        let dir = tempfile::tempdir().unwrap();
        let mut h = Harness::open_paused(dir.path().join("db"), options()).unwrap();
        let id = h
            .create_conversation(Config {
                model: "test".into(),
                system_prompt: "".into(),
                max_output_tokens: 10,
            })
            .unwrap();
        let occurrence = h
            .append_user_blocks(id, vec![serde_json::json!({"type":"text", "text":"cut"})])
            .unwrap();
        let cursor = h
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
        h.fork_by_entry_id(id, occurrence).unwrap();
        let mut observer = b"durable/observer/".to_vec();
        observer.extend_from_slice(id.as_bytes());
        observer.extend_from_slice(b"/r");
        observer.extend_from_slice(&cursor.0.to_be_bytes());
        let fork = format!("runtime/fork/{id}/{:020}", cursor.0).into_bytes();
        let keys = match mode {
            0 => vec![observer],
            1 => vec![fork],
            _ => vec![observer, fork],
        };
        let changes: Vec<_> = keys
            .into_iter()
            .map(|key| {
                let value = h.owner.store().get(&key).unwrap().unwrap().value;
                Mutation::Put { key, value }
            })
            .collect();
        h.owner.store().commit(&[], &changes).unwrap();
        assert!(h.fork_by_entry_id(id, occurrence).is_err());
    }
}

#[test]
fn anchored_reset_derivation_replays_committed_prefix_and_rejects_tampering() {
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
    let address = crate::document::Address {
        scope: id.to_string(),
        name: "proof-doc".into(),
        key: "".into(),
    };
    let mut document = crate::document::Transaction::new();
    document
        .put(
            &address,
            Expected::Missing,
            serde_json::json!({"version":1}).as_object().unwrap(),
        )
        .unwrap();
    document.commit(h.owner.store()).unwrap();
    h.submit(id, None, "active".into()).unwrap();
    let blocks1 = vec![serde_json::json!({"type":"text","text":"first"})];
    let blocks2 = vec![serde_json::json!({"type":"text","text":"second"})];
    let (r1, first_ids) = h
        .reset_with_blocks(id, Some("one".into()), vec![blocks1.clone()])
        .unwrap();
    let w1 = h
        .submit_write(id, None, "first.write".into(), None)
        .unwrap();
    let (r2, second_ids) = h
        .reset_with_blocks(id, Some("two".into()), vec![blocks2.clone()])
        .unwrap();
    let w2 = h
        .submit_write(id, None, "second.write".into(), None)
        .unwrap();
    drop(h);
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let (before, version) = h.load(id).unwrap();
    let steps = [
        ResetDerivationStep {
            reset_id: r1,
            context_generation: Uuid::new_v4(),
            write_limit: usize::MAX,
        },
        ResetDerivationStep {
            reset_id: r2,
            context_generation: Uuid::new_v4(),
            write_limit: usize::MAX,
        },
    ];
    let cuts = h.derive_anchored_reset_states(id, version, &steps).unwrap();
    assert_eq!(cuts.len(), 2);
    assert_eq!(
        cuts[0].resets.iter().map(|r| r.id).collect::<Vec<_>>(),
        vec![r1]
    );
    assert_eq!(
        cuts[0]
            .pending_resets
            .iter()
            .map(|r| r.id)
            .collect::<Vec<_>>(),
        vec![r2]
    );
    assert_eq!(cuts[0].content_blocks[&1].message_id, first_ids[0]);
    assert_eq!(cuts[0].content_blocks[&1].blocks, blocks1);
    assert!(
        cuts[0]
            .application_writes
            .iter()
            .find(|w| w.id == w1)
            .unwrap()
            .placed
    );
    assert!(
        !cuts[0]
            .application_writes
            .iter()
            .find(|w| w.id == w2)
            .unwrap()
            .placed
    );
    assert_eq!(
        cuts[1].resets.iter().map(|r| r.id).collect::<Vec<_>>(),
        vec![r1, r2]
    );
    assert!(cuts[1].pending_resets.is_empty());
    assert_eq!(cuts[1].content_blocks[&1].message_id, second_ids[0]);
    assert_eq!(cuts[1].content_blocks[&1].blocks, blocks2);
    assert!(
        cuts[1]
            .application_writes
            .iter()
            .find(|w| w.id == w2)
            .unwrap()
            .placed
    );
    assert_eq!(h.conversation(id).unwrap().resets, before.resets);
    assert!(
        h.derive_anchored_reset_states(id, version, &[steps[1], steps[0]])
            .is_err()
    );
    assert!(
        h.derive_anchored_reset_states(
            id,
            version,
            &[
                steps[0],
                ResetDerivationStep {
                    context_generation: steps[0].context_generation,
                    ..steps[1]
                }
            ]
        )
        .is_err()
    );
    assert!(
        h.derive_anchored_reset_states(id, version + 1, &steps)
            .is_err()
    );
    let head = h
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
    let key = format!("runtime/fork/{id}/{:020}", head.0).into_bytes();
    let fork = h.fork_at(id, head).unwrap();
    assert_eq!(h.conversation(fork).unwrap().messages, before.messages);
    let revision = h.owner.store().document_cut(&key).unwrap().unwrap();
    let document_version = h.document(&address).unwrap().unwrap().version;
    let mut document = crate::document::Transaction::new();
    document
        .put(
            &address,
            Expected::Version(document_version),
            serde_json::json!({"version":2}).as_object().unwrap(),
        )
        .unwrap();
    document.commit(h.owner.store()).unwrap();
    h.submit(id, None, "later".into()).unwrap();
    assert_eq!(
        h.document_at_cut(id, head, &address)
            .unwrap()
            .unwrap()
            .value["version"],
        1
    );
    let (historical, cut_revision) = h.authenticated_reset_baseline(id, head.0).unwrap();
    assert_eq!(cut_revision, revision);
    assert_eq!(
        serde_json::to_value(&historical).unwrap(),
        serde_json::to_value(&before).unwrap()
    );
    assert_eq!(historical.messages, before.messages);
    assert_eq!(historical.content_blocks, before.content_blocks);
    assert_eq!(
        derive_ordered_reset_states(&historical, &steps).unwrap()[1].content_blocks[&1].message_id,
        second_ids[0]
    );
    let entry = h.owner.store().get(&key).unwrap().unwrap();
    h.owner
        .store()
        .commit(
            &[],
            &[Mutation::Put {
                key,
                value: entry.value,
            }],
        )
        .unwrap();
    assert!(h.authenticated_reset_baseline(id, head.0).is_err());
}

#[cfg(unix)]
#[test]
fn reset_baseline_kill_child() {
    use std::io::Write;
    let Ok(path) = std::env::var("PI_RESET_BASELINE_KILL_DB") else {
        return;
    };
    let mut h = Harness::open_paused(path, options()).unwrap();
    let id = h
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 10,
        })
        .unwrap();
    h.submit(id, None, "active".into()).unwrap();
    let (reset, ids) = h
        .reset_with_blocks(
            id,
            Some("handoff".into()),
            vec![vec![serde_json::json!({"type":"text","text":"saved"})]],
        )
        .unwrap();
    let head = h
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
    println!(
        "RESET_BASELINE_COMMITTED {id} {} {reset} {}",
        head.0, ids[0]
    );
    std::io::stdout().flush().unwrap();
    loop {
        std::thread::park();
    }
}

#[cfg(unix)]
#[test]
fn authenticated_reset_baseline_survives_process_kill_and_reopen() {
    use std::io::{BufRead, BufReader};
    use std::process::{Command, Stdio};
    use std::time::Duration;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "runtime::runtime_cas_tests::reset_baseline_kill_child",
            "--nocapture",
        ])
        .env("PI_RESET_BASELINE_KILL_DB", &path)
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let stdout = child.stdout.take().unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines().map_while(Result::ok) {
            if let Some((_, rest)) = line.split_once("RESET_BASELINE_COMMITTED ") {
                let _ = tx.send(rest.to_string());
                break;
            }
        }
    });
    let marker = rx.recv_timeout(Duration::from_secs(20));
    child.kill().unwrap();
    child.wait().unwrap();
    let marker = marker.expect("child did not commit baseline");
    let mut fields = marker.split_whitespace();
    let id = Uuid::parse_str(fields.next().unwrap()).unwrap();
    let head: u64 = fields.next().unwrap().parse().unwrap();
    let reset = Uuid::parse_str(fields.next().unwrap()).unwrap();
    let occurrence = Uuid::parse_str(fields.next().unwrap()).unwrap();
    let h = Harness::open_paused(&path, options()).unwrap();
    let (baseline, _) = h.authenticated_reset_baseline(id, head).unwrap();
    assert_eq!(baseline.pending_resets[0].id, reset);
    assert_eq!(
        baseline.pending_reset_blocks[&reset][0].message_id,
        occurrence
    );
    let steps = [ResetDerivationStep {
        reset_id: reset,
        context_generation: Uuid::new_v4(),
        write_limit: usize::MAX,
    }];
    let cuts = derive_ordered_reset_states(&baseline, &steps).unwrap();
    assert_eq!(cuts[0].content_blocks[&1].message_id, occurrence);
}

#[test]
fn reset_baseline_digest_rejects_forged_same_commit_anchor() {
    let dir = tempfile::tempdir().unwrap();
    let mut h = Harness::open_paused(dir.path().join("db"), options()).unwrap();
    let id = h
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 10,
        })
        .unwrap();
    h.submit(id, None, "active".into()).unwrap();
    h.reset_with_blocks(
        id,
        None,
        vec![vec![serde_json::json!({"type":"text", "text":"one"})]],
    )
    .unwrap();
    let head = h
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
    h.authenticated_reset_baseline(id, head.0).unwrap();
    let root = Harness::key(id);
    let fork = format!("runtime/fork/{id}/{:020}", head.0).into_bytes();
    let mut event = b"durable/observer/".to_vec();
    event.extend_from_slice(id.as_bytes());
    event.extend_from_slice(b"/r");
    event.extend_from_slice(&head.0.to_be_bytes());
    let root_value = h.owner.store().get(&root).unwrap().unwrap().value;
    let fork_value = h.owner.store().get(&fork).unwrap().unwrap().value;
    let mut event_value: serde_json::Value =
        serde_json::from_slice(&h.owner.store().get(&event).unwrap().unwrap().value).unwrap();
    event_value["_reset_baseline_sha256"] = serde_json::json!("forged");
    h.owner
        .store()
        .commit(
            &[],
            &[
                Mutation::Put {
                    key: root,
                    value: root_value,
                },
                Mutation::Put {
                    key: fork,
                    value: fork_value,
                },
                Mutation::Put {
                    key: event,
                    value: serde_json::to_vec(&event_value).unwrap(),
                },
            ],
        )
        .unwrap();
    h.authenticate_fork_anchor(id, head.0).unwrap();
    assert!(h.authenticated_reset_baseline(id, head.0).is_err());
}

#[test]
fn legacy_undigested_baseline_is_only_usable_at_current_root() {
    let dir = tempfile::tempdir().unwrap();
    let mut h = Harness::open_paused(dir.path().join("db"), options()).unwrap();
    let id = h
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 10,
        })
        .unwrap();
    h.submit(id, None, "active".into()).unwrap();
    let (reset, _) = h
        .reset_with_blocks(
            id,
            None,
            vec![vec![serde_json::json!({"type":"text", "text":"one"})]],
        )
        .unwrap();
    let head = h
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
    let root = Harness::key(id);
    let fork = format!("runtime/fork/{id}/{:020}", head.0).into_bytes();
    let mut event = b"durable/observer/".to_vec();
    event.extend_from_slice(id.as_bytes());
    event.extend_from_slice(b"/r");
    event.extend_from_slice(&head.0.to_be_bytes());
    let root_value = h.owner.store().get(&root).unwrap().unwrap().value;
    let fork_value = h.owner.store().get(&fork).unwrap().unwrap().value;
    let mut event_value: serde_json::Value =
        serde_json::from_slice(&h.owner.store().get(&event).unwrap().unwrap().value).unwrap();
    event_value
        .as_object_mut()
        .unwrap()
        .remove("_reset_baseline_sha256");
    h.owner
        .store()
        .commit(
            &[],
            &[
                Mutation::Put {
                    key: root,
                    value: root_value,
                },
                Mutation::Put {
                    key: fork,
                    value: fork_value,
                },
                Mutation::Put {
                    key: event,
                    value: serde_json::to_vec(&event_value).unwrap(),
                },
            ],
        )
        .unwrap();
    let (_, version) = h.load(id).unwrap();
    let steps = [ResetDerivationStep {
        reset_id: reset,
        context_generation: Uuid::new_v4(),
        write_limit: usize::MAX,
    }];
    assert_eq!(
        h.derive_anchored_reset_states(id, version, &steps)
            .unwrap()
            .len(),
        1
    );
    assert!(h.authenticated_reset_baseline(id, head.0).is_err());
}
