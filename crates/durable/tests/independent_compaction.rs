use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use futures_util::{StreamExt, stream};
use pi_rs_core::{Model, ModelEvent, ModelRequest, ModelStream};
use pi_rs_durable::recovery::UncertainPolicy;
use pi_rs_durable::runtime::{
    Config, IndependentCompactionStatus, Options, Scheduler, SummaryProviderIdentity,
};

#[derive(Default)]
struct ModelStub {
    summaries: AtomicUsize,
}

#[async_trait::async_trait]
impl Model for ModelStub {
    async fn stream(&self, _: ModelRequest) -> pi_rs_core::Result<ModelStream> {
        Ok(stream::iter(vec![
            Ok(ModelEvent::Text("answer".into())),
            Ok(ModelEvent::Complete {
                calls: vec![],
                usage: None,
            }),
        ])
        .boxed())
    }
    async fn stream_for_summary(
        &self,
        _: uuid::Uuid,
        _: uuid::Uuid,
        _: ModelRequest,
    ) -> pi_rs_core::Result<ModelStream> {
        self.summaries.fetch_add(1, Ordering::SeqCst);
        Ok(stream::iter(vec![
            Ok(ModelEvent::Text("checkpoint".into())),
            Ok(ModelEvent::Complete {
                calls: vec![],
                usage: None,
            }),
        ])
        .boxed())
    }
}

#[tokio::test]
async fn public_api_submits_and_places_a_summary() {
    let dir = tempfile::tempdir().unwrap();
    let model = Arc::new(ModelStub::default());
    let options = Options {
        model: model.clone(),
        tools: vec![],
        restart_limit: 3,
        max_turns: 3,
        uncertain_policy: UncertainPolicy::InterruptedResult,
    };
    let provider = SummaryProviderIdentity {
        model: "example:model:v1".into(),
        settlement: "example:ledger:v1".into(),
    };
    let mut scheduler = Scheduler::open_with_summary_recovery(
        dir.path().join("db"),
        options,
        Default::default(),
        None,
        provider,
    )
    .unwrap();
    let conversation = scheduler
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: String::new(),
            max_output_tokens: 64,
        })
        .unwrap();
    scheduler
        .submit(conversation, None, "first".into())
        .unwrap();
    scheduler.wait(conversation).await.unwrap();
    scheduler
        .submit(conversation, None, "second".into())
        .unwrap();
    scheduler.wait(conversation).await.unwrap();
    let operation = uuid::Uuid::new_v4();
    assert!(
        scheduler
            .request_idle_independent_compaction(conversation, operation, 0)
            .unwrap()
    );
    assert_eq!(
        scheduler
            .wait_independent_compaction(operation)
            .await
            .unwrap(),
        IndependentCompactionStatus::Applied
    );
    assert_eq!(model.summaries.load(Ordering::SeqCst), 1);
    assert!(
        scheduler
            .request_idle_independent_compaction(conversation, operation, 0)
            .is_err()
    );
    assert_eq!(
        scheduler.independent_compaction_status(operation).unwrap(),
        IndependentCompactionStatus::Applied
    );
    let conversation = scheduler.conversation(conversation).unwrap();
    assert!(conversation.messages.iter().any(|message| matches!(message, pi_rs_core::Message::User { content } if content.contains("<summary>\ncheckpoint"))));
    scheduler.shutdown().await.unwrap();
}
