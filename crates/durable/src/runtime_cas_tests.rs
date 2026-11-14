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
    assert_eq!(latest.submissions.len(), 2);
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
            let mut worker = owner.worker();
            worker.merge_admissions = false;
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
