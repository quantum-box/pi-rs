use async_trait::async_trait;
use futures_util::{StreamExt, stream};
use pi_rs_core::{CancellationToken, Error, Message, Model, ModelEvent, ModelRequest, ModelStream};
use pi_rs_durable::{
    observer::Cursor,
    recovery::UncertainPolicy,
    runtime::{Config, Harness, Options},
};
use std::{num::NonZeroUsize, sync::Arc};
struct ModelStub;
#[async_trait]
impl Model for ModelStub {
    async fn stream(&self, request: ModelRequest) -> Result<ModelStream, Error> {
        let summary=request.messages.iter().any(|m|matches!(m,Message::System{content} if content.starts_with("You are a context summarization")));
        let text = if summary {
            "summary of prior work"
        } else {
            "answer"
        };
        Ok(stream::iter(vec![
            Ok(ModelEvent::Text(text.into())),
            Ok(ModelEvent::Complete {
                calls: vec![],
                usage: None,
            }),
        ])
        .boxed())
    }
}
fn opts() -> Options {
    Options {
        model: Arc::new(ModelStub),
        tools: vec![],
        restart_limit: 5,
        max_turns: 5,
        uncertain_policy: UncertainPolicy::InterruptedResult,
    }
}
#[tokio::test]
async fn compaction_is_durable_and_old_snapshots_remain_observable() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let cancel = CancellationToken::new();
    let id;
    {
        let mut h = Harness::open(&path, opts(), &cancel).await.unwrap();
        id = h
            .create_conversation(Config {
                model: "test".into(),
                system_prompt: "".into(),
                max_output_tokens: 128,
            })
            .unwrap();
        h.submit_and_run(id, None, "first question".into(), &cancel)
            .await
            .unwrap();
        h.submit_and_run(id, None, "latest question".into(), &cancel)
            .await
            .unwrap();
        assert!(h.request_compaction(id, 0).unwrap());
        h.resume(&cancel).await.unwrap();
    }
    let h = Harness::open(&path, opts(), &cancel).await.unwrap();
    let c = h.conversation(id).unwrap();
    assert!(
        c.messages
            .iter()
            .any(|m| matches!(m,Message::User{content} if content=="latest question"))
    );
    assert!(
        c.messages.iter().any(
            |m| matches!(m,Message::User{content} if content.contains("summary of prior work"))
        )
    );
    let page = h
        .history(id, Cursor(0), NonZeroUsize::new(1000).unwrap())
        .unwrap();
    assert!(
        page.records
            .iter()
            .any(|r| r.value.to_string().contains("first question"))
    );
}

#[tokio::test]
async fn fork_copies_settled_context_but_not_submissions() {
    let dir = tempfile::tempdir().unwrap();
    let cancel = CancellationToken::new();
    let mut h = Harness::open(dir.path().join("db"), opts(), &cancel)
        .await
        .unwrap();
    let parent = h
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: "".into(),
            max_output_tokens: 128,
        })
        .unwrap();
    h.submit_and_run(parent, Some("request".into()), "original".into(), &cancel)
        .await
        .unwrap();
    let fork = h.fork(parent).unwrap();
    assert!(h.conversation(fork).unwrap().submissions.is_empty());
    assert_eq!(
        h.conversation(fork).unwrap().messages,
        h.conversation(parent).unwrap().messages
    );
    h.submit_and_run(fork, Some("request".into()), "forked".into(), &cancel)
        .await
        .unwrap();
    assert_eq!(h.conversation(parent).unwrap().submissions.len(), 1);
    assert_eq!(h.conversation(fork).unwrap().submissions.len(), 1);
    assert_ne!(
        h.conversation(fork).unwrap().messages,
        h.conversation(parent).unwrap().messages
    );
}
