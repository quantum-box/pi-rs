use async_trait::async_trait;
use futures_util::{StreamExt, stream};
use pi_rs_core::{CancellationToken, Error, Message, Model, ModelEvent, ModelRequest, ModelStream};
use pi_rs_durable::{
    recovery::UncertainPolicy,
    runtime::{Config, Harness, Options, Status},
};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
struct PartialModel {
    calls: AtomicUsize,
    reached: Arc<tokio::sync::Notify>,
}
#[async_trait]
impl Model for PartialModel {
    async fn stream(&self, request: ModelRequest) -> Result<ModelStream, Error> {
        let attempt = self.calls.fetch_add(1, Ordering::SeqCst);
        assert!(!request.messages.iter().any(|m|matches!(m,Message::Assistant{content,..} if content.contains("uncommitted partial"))));
        if attempt == 0 {
            let reached = self.reached.clone();
            let events = stream::unfold((0, reached), |(n, reached)| async move {
                if n == 0 {
                    Some((
                        Ok(ModelEvent::Text("uncommitted partial".into())),
                        (1, reached),
                    ))
                } else {
                    reached.notify_one();
                    std::future::pending().await
                }
            });
            Ok(events.boxed())
        } else {
            Ok(stream::iter(vec![
                Ok(ModelEvent::Text("finished".into())),
                Ok(ModelEvent::Complete {
                    calls: vec![],
                    usage: None,
                }),
            ])
            .boxed())
        }
    }
}
fn opts(model: Arc<PartialModel>) -> Options {
    Options {
        model,
        tools: vec![],
        restart_limit: 5,
        max_turns: 8,
        uncertain_policy: UncertainPolicy::InterruptedResult,
    }
}
#[tokio::test]
async fn committed_partial_becomes_aborted_display_entry_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let cancel = CancellationToken::new();
    let reached = Arc::new(tokio::sync::Notify::new());
    let model = Arc::new(PartialModel {
        calls: AtomicUsize::new(0),
        reached: reached.clone(),
    });
    let id;
    {
        let mut h = Harness::open(&path, opts(model.clone()), &cancel)
            .await
            .unwrap();
        id = h
            .create_conversation(Config {
                model: "test".into(),
                system_prompt: "".into(),
                max_output_tokens: 100,
            })
            .unwrap();
        h.submit(id, None, "work".into()).unwrap();
        {
            let run = h.resume(&cancel);
            tokio::pin!(run);
            tokio::select! {r=&mut run=>panic!("unexpected completion {r:?}"),_=reached.notified()=>{}}
        }
    }
    let h = Harness::open(&path, opts(model.clone()), &cancel)
        .await
        .unwrap();
    let c = h.conversation(id).unwrap();
    let view = h
        .view(id, std::num::NonZeroUsize::new(50).unwrap())
        .unwrap();
    assert_eq!(
        view.state.aborted_generations[0].text,
        "uncommitted partial"
    );
    assert_eq!(c.aborted_generations.len(), 1);
    assert_eq!(c.aborted_generations[0].text, "uncommitted partial");
    assert_eq!(
        c.submissions[0].status,
        Status::Done {
            answer: "finished".into()
        }
    );
    assert_eq!(model.calls.load(Ordering::SeqCst), 2);
}
#[tokio::test]
async fn view_shows_committed_live_partial() {
    let dir = tempfile::tempdir().unwrap();
    let reached = Arc::new(tokio::sync::Notify::new());
    let model = Arc::new(PartialModel {
        calls: AtomicUsize::new(0),
        reached: reached.clone(),
    });
    let mut scheduler =
        pi_rs_durable::runtime::Scheduler::open(dir.path().join("db"), opts(model)).unwrap();
    let id = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    scheduler.submit(id, None, "work".into()).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), reached.notified())
        .await
        .unwrap();
    let view = scheduler
        .view(id, std::num::NonZeroUsize::new(50).unwrap())
        .unwrap();
    assert!(
        matches!(view.live, Some(pi_rs_durable::progress::State::Running(ref partial)) if partial.text == "uncommitted partial")
    );
    scheduler.abort_and_wait(id).await.unwrap();
    scheduler.shutdown().await.unwrap();
}
struct EscapedModel;
#[async_trait]
impl Model for EscapedModel {
    async fn stream(&self, _: ModelRequest) -> Result<ModelStream, Error> {
        let text = "\\".repeat(600_000);
        Ok(stream::iter(vec![
            Ok(ModelEvent::Text(text)),
            Ok(ModelEvent::Complete {
                calls: vec![],
                usage: None,
            }),
        ])
        .boxed())
    }
}
#[tokio::test]
async fn oversized_escaped_progress_does_not_cancel_a_valid_answer() {
    let dir = tempfile::tempdir().unwrap();
    let cancel = CancellationToken::new();
    let options = Options {
        model: Arc::new(EscapedModel),
        tools: vec![],
        restart_limit: 5,
        max_turns: 8,
        uncertain_policy: UncertainPolicy::InterruptedResult,
    };
    let mut h = Harness::open(dir.path().join("db"), options, &cancel)
        .await
        .unwrap();
    let id = h
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 100,
        })
        .unwrap();
    h.submit_and_run(id, None, "work".into(), &cancel)
        .await
        .unwrap();
    assert!(
        matches!(&h.conversation(id).unwrap().submissions[0].status,Status::Done{answer} if answer.len()==600_000)
    );
}
