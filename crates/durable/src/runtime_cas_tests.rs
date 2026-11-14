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
