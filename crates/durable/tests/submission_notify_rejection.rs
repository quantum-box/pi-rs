use async_trait::async_trait;
use pi_rs_core::{Error as ModelError, Model, ModelRequest, ModelStream};
use pi_rs_durable::{
    recovery::UncertainPolicy,
    runtime::{AdmissionOptions, Config, Error, Harness, Options, Scheduler, SubmissionNotify},
};
use std::sync::Arc;
use uuid::Uuid;

struct Stub;
#[async_trait]
impl Model for Stub {
    async fn stream(&self, _: ModelRequest) -> Result<ModelStream, ModelError> {
        panic!("rejected notifications must not schedule model work")
    }
}
fn options() -> Options {
    Options {
        model: Arc::new(Stub),
        tools: vec![],
        restart_limit: 2,
        max_turns: 2,
        uncertain_policy: UncertainPolicy::InterruptedResult,
    }
}
fn config() -> Config {
    Config {
        model: "test".into(),
        system_prompt: String::new(),
        max_output_tokens: 10,
    }
}
fn notify() -> SubmissionNotify {
    SubmissionNotify {
        task_id: Uuid::from_u128(42),
        key: "answer".into(),
    }
}

#[test]
fn notification_requests_are_rejected_before_admission_or_request_id_reservation() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let input = h.create_conversation(config()).unwrap();
    let continuation = h.create_conversation(config()).unwrap();
    assert!(matches!(
        h.submit_with_notify(
            input,
            Some("input".into()),
            "hello".into(),
            AdmissionOptions::default(),
            Some(notify())
        ),
        Err(Error::SubmissionNotificationUnsupported)
    ));
    assert!(matches!(
        h.submit_continue_with_notify(continuation, Some("continue".into()), Some(notify())),
        Err(Error::SubmissionNotificationUnsupported)
    ));
    assert!(h.conversation(input).unwrap().submissions.is_empty());
    assert!(h.conversation(continuation).unwrap().submissions.is_empty());
    let input_id = h
        .submit_with_notify(
            input,
            Some("input".into()),
            "hello".into(),
            AdmissionOptions::default(),
            None,
        )
        .unwrap();
    let continuation_id = h
        .submit_continue_with_notify(continuation, Some("continue".into()), None)
        .unwrap();
    drop(h);
    let h = Harness::open_paused(&path, options()).unwrap();
    assert_eq!(h.conversation(input).unwrap().submissions[0].id, input_id);
    assert_eq!(
        h.conversation(continuation).unwrap().submissions[0].id,
        continuation_id
    );
}

#[tokio::test]
async fn scheduler_does_not_schedule_rejected_notification() {
    let dir = tempfile::tempdir().unwrap();
    let mut scheduler = Scheduler::open(dir.path().join("db"), options()).unwrap();
    let id = scheduler.create_conversation(config()).unwrap();
    assert!(matches!(
        scheduler.submit_with_notify(
            id,
            Some("input".into()),
            "hello".into(),
            AdmissionOptions::default(),
            Some(notify())
        ),
        Err(Error::SubmissionNotificationUnsupported)
    ));
    assert!(matches!(
        scheduler.submit_continue_with_notify(id, Some("continue".into()), Some(notify())),
        Err(Error::SubmissionNotificationUnsupported)
    ));
    assert!(scheduler.conversation(id).unwrap().submissions.is_empty());
    scheduler.shutdown().await.unwrap();
}
