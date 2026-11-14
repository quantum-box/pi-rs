use async_trait::async_trait;
use futures_util::{StreamExt, stream};
use pi_rs_core::{CancellationToken, Error, Message, Model, ModelEvent, ModelRequest, ModelStream};
use pi_rs_durable::{
    recovery::UncertainPolicy,
    runtime::{Config, Harness, Options, Status, SubmissionKind},
    state::AdmissionError,
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

struct Answer(AtomicUsize, bool);
#[async_trait]
impl Model for Answer {
    async fn stream(&self, request: ModelRequest) -> Result<ModelStream, Error> {
        let n = self.0.fetch_add(1, Ordering::SeqCst);
        assert!(
            request
                .messages
                .iter()
                .any(|m| matches!(m, Message::User { .. }))
        );
        if self.1 && n == 0 {
            return Ok(stream::iter(vec![Ok(ModelEvent::Complete {
                calls: vec![pi_rs_core::ToolCall {
                    id: "missing".into(),
                    name: "missing".into(),
                    arguments: "{}".into(),
                }],
                usage: None,
            })])
            .boxed());
        }
        Ok(stream::iter(vec![
            Ok(ModelEvent::Text("answer".into())),
            Ok(ModelEvent::Complete {
                calls: vec![],
                usage: None,
            }),
        ])
        .boxed())
    }
}
fn options(model: Arc<Answer>, max_turns: u32) -> Options {
    Options {
        model,
        tools: vec![],
        restart_limit: 2,
        max_turns,
        uncertain_policy: UncertainPolicy::InterruptedResult,
    }
}
fn config() -> Config {
    Config {
        model: "test".into(),
        system_prompt: "system".into(),
        max_output_tokens: 20,
    }
}

#[tokio::test]
async fn empty_answered_busy_and_reopen_are_idempotent() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let model = Arc::new(Answer(AtomicUsize::new(0), false));
    let cancel = CancellationToken::new();
    let mut h = Harness::open_paused(&path, options(model.clone(), 3)).unwrap();
    let id = h.create_conversation(config()).unwrap();
    let empty = h.submit_continue(id, Some("empty".into())).unwrap();
    assert!(matches!(
        h.submit_continue(id, Some("busy".into())),
        Err(pi_rs_durable::runtime::Error::Admission(
            AdmissionError::Busy
        ))
    ));
    assert_eq!(h.submit_continue(id, Some("empty".into())).unwrap(), empty);
    drop(h);
    let mut h = Harness::open(&path, options(model.clone(), 3), &cancel)
        .await
        .unwrap();
    let c = h.conversation(id).unwrap();
    assert!(c.messages.is_empty());
    assert_eq!(c.submissions[0].kind, SubmissionKind::Continue);
    assert_eq!(
        c.submissions[0].status,
        Status::Unanswered {
            reason: "nothing_to_answer".into()
        }
    );
    assert_eq!(model.0.load(Ordering::SeqCst), 0);
    h.submit_and_run(id, None, "hello".into(), &cancel)
        .await
        .unwrap();
    let after = h.submit_continue(id, Some("after".into())).unwrap();
    h.resume(&cancel).await.unwrap();
    assert_eq!(model.0.load(Ordering::SeqCst), 1);
    assert_eq!(
        h.conversation(id)
            .unwrap()
            .submissions
            .iter()
            .find(|s| s.id == after)
            .unwrap()
            .status,
        Status::Unanswered {
            reason: "nothing_to_answer".into()
        }
    );
}

#[tokio::test]
async fn continue_after_failure_places_queued_input_and_recovers_once() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let model = Arc::new(Answer(AtomicUsize::new(0), true));
    let cancel = CancellationToken::new();
    let mut h = Harness::open_paused(&path, options(model.clone(), 1)).unwrap();
    let id = h.create_conversation(config()).unwrap();
    h.submit(id, None, "first".into()).unwrap();
    let queued = h.submit(id, None, "queued".into()).unwrap();
    h.resume(&cancel).await.unwrap();
    assert_eq!(
        h.conversation(id).unwrap().submissions[1].status,
        Status::Queued
    );
    let continuation = h.submit_continue(id, Some("resume".into())).unwrap();
    drop(h);
    let mut h = Harness::open(&path, options(model.clone(), 3), &cancel)
        .await
        .unwrap();
    let c = h.conversation(id).unwrap();
    assert_eq!(
        c.messages
            .iter()
            .filter(|m| matches!(m, Message::User { .. }))
            .count(),
        2
    );
    for sid in [queued, continuation] {
        assert_eq!(
            c.submissions.iter().find(|s| s.id == sid).unwrap().status,
            Status::Done {
                answer: "answer".into()
            }
        );
    }
    assert_eq!(c.answer_id(queued), c.answer_id(continuation));
    assert_eq!(
        h.submit_continue(id, Some("resume".into())).unwrap(),
        continuation
    );
    h.resume(&cancel).await.unwrap();
    assert_eq!(model.0.load(Ordering::SeqCst), 2);
}

struct Truncated(AtomicUsize);
#[async_trait]
impl Model for Truncated {
    async fn stream(&self, request: ModelRequest) -> Result<ModelStream, Error> {
        let n = self.0.fetch_add(1, Ordering::SeqCst);
        let events = if n == 0 {
            vec![
                Ok(ModelEvent::Metadata(pi_rs_core::ModelMetadata {
                    finish_reason: Some("length".into()),
                    ..Default::default()
                })),
                Ok(ModelEvent::Complete {
                    calls: vec![pi_rs_core::ToolCall {
                        id: "truncated".into(),
                        name: "unsafe".into(),
                        arguments: "{".into(),
                    }],
                    usage: None,
                }),
            ]
        } else {
            assert!(
                matches!(request.messages.last(), Some(Message::Tool { call_id, .. }) if call_id == "truncated")
            );
            vec![
                Ok(ModelEvent::Text("recovered".into())),
                Ok(ModelEvent::Complete {
                    calls: vec![],
                    usage: None,
                }),
            ]
        };
        Ok(stream::iter(events).boxed())
    }
}

#[tokio::test]
async fn truncated_call_becomes_result_without_dispatch_and_can_continue() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let model = Arc::new(Truncated(AtomicUsize::new(0)));
    let opts = Options {
        model: model.clone(),
        tools: vec![],
        restart_limit: 2,
        max_turns: 3,
        uncertain_policy: UncertainPolicy::InterruptedResult,
    };
    let cancel = CancellationToken::new();
    let mut h = Harness::open_paused(&path, opts).unwrap();
    let id = h.create_conversation(config()).unwrap();
    h.submit_and_run(id, None, "work".into(), &cancel)
        .await
        .unwrap();
    assert!(
        matches!(h.conversation(id).unwrap().messages.last(), Some(Message::Tool { call_id, .. }) if call_id == "truncated")
    );
    let sid = h.submit_continue(id, None).unwrap();
    h.resume(&cancel).await.unwrap();
    assert_eq!(
        h.conversation(id)
            .unwrap()
            .submissions
            .iter()
            .find(|s| s.id == sid)
            .unwrap()
            .status,
        Status::Done {
            answer: "recovered".into()
        }
    );
    assert_eq!(model.0.load(Ordering::SeqCst), 2);
}
