use futures_util::stream;
use pi_rs_core::{
    Agent, CancellationToken, EnrichedModelEvent as E, EnrichedModelStream, Error, Event, Limits,
    Model, ModelApi, ModelEvent, ModelMetadata, ModelRequest, ModelStream, Record, Registry,
    Result, Session, SessionStore, Usage,
};
use uuid::Uuid;

struct Fixture {
    fail: bool,
    incomplete: bool,
}
#[async_trait::async_trait]
impl Model for Fixture {
    async fn stream(&self, _: ModelRequest) -> Result<ModelStream> {
        unreachable!()
    }
    async fn stream_enriched_in_session_with_identity(
        &self,
        _: Uuid,
        _: Uuid,
        _: &str,
        _: ModelRequest,
    ) -> Result<EnrichedModelStream> {
        let mut events = vec![
            Ok(E::Legacy(ModelEvent::Metadata(ModelMetadata {
                provider_api: Some(ModelApi::AnthropicMessages),
                ..Default::default()
            }))),
            Ok(E::TextStart { index: 0 }),
            Ok(E::TextDelta {
                index: 0,
                delta: "first".into(),
            }),
            Ok(E::TextEnd {
                index: 0,
                content: "first".into(),
            }),
            Ok(E::TextStart { index: 1 }),
            Ok(E::TextDelta {
                index: 1,
                delta: "second".into(),
            }),
        ];
        if !self.incomplete {
            events.push(Ok(E::TextEnd {
                index: 1,
                content: "second".into(),
            }));
        }
        if self.fail {
            events.push(Err(Error::Cancelled));
        } else {
            events.push(Ok(E::Legacy(ModelEvent::Complete {
                calls: vec![],
                usage: Some(Usage {
                    input_tokens: 1,
                    output_tokens: 1,
                }),
            })));
        }
        Ok(Box::pin(stream::iter(events)))
    }
}
#[derive(Default)]
struct Legacy(Vec<Record>);
impl SessionStore for Legacy {
    fn load(&mut self) -> Result<Vec<Record>> {
        Ok(self.0.clone())
    }
    fn append(&mut self, record: &Record) -> Result<()> {
        self.0.push(record.clone());
        Ok(())
    }
}
#[derive(Default)]
struct TinyStore(Vec<Record>);
impl SessionStore for TinyStore {
    fn load(&mut self) -> Result<Vec<Record>> {
        Ok(self.0.clone())
    }
    fn append(&mut self, record: &Record) -> Result<()> {
        self.0.push(record.clone());
        Ok(())
    }
    fn supports_record_metadata(&self) -> bool {
        true
    }
    fn encoded_record_limit(&self) -> Option<usize> {
        Some(1)
    }
    fn append_with_metadata(
        &mut self,
        record: &Record,
        metadata: Option<&serde_json::Value>,
    ) -> Result<()> {
        assert!(
            metadata.is_none(),
            "oversized advisory metadata must be omitted before writing"
        );
        self.append(record)
    }
}
async fn run(session: &mut Session, fail: bool, incomplete: bool) {
    let model = Fixture { fail, incomplete };
    let registry = Registry::default();
    let agent = Agent {
        model: &model,
        model_name: "fixture",
        registry: &registry,
        system_prompt: "",
        limits: Limits::default(),
    };
    let (_sender, mut inputs) = tokio::sync::mpsc::channel(1);
    let outcome = agent
        .run_with_events(
            session,
            Some("hello".into()),
            &mut inputs,
            &CancellationToken::new(),
            &mut |_| {},
        )
        .await;
    assert_eq!(outcome.is_err(), fail);
}
fn completion_id(session: &Session) -> u64 {
    session
        .records()
        .iter()
        .find(|r| matches!(r.event, Event::ModelFinished { .. }))
        .unwrap()
        .id
}
#[tokio::test]
async fn layout_persists_on_reopen_and_legacy_stores_fall_back() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("session.jsonl");
    let mut session = Session::open(&path).unwrap();
    run(&mut session, false, false).await;
    let id = completion_id(&session);
    assert_eq!(
        session.record_metadata(id).unwrap()["anthropicDisplayLayoutV1"],
        serde_json::json!([
        {"type":"text","text":"first"},{"type":"text","text":"second"}])
    );
    drop(session);
    let reopened = Session::open(&path).unwrap();
    assert!(reopened.record_metadata(id).is_some());
    let fork = reopened.fork_session(reopened.head()).unwrap();
    assert!(fork.record_metadata(completion_id(&fork)).is_some());
    let mut legacy = Session::with_store(Legacy::default()).unwrap();
    run(&mut legacy, false, false).await;
    assert!(legacy.record_metadata(completion_id(&legacy)).is_none());
    let mut tiny = Session::with_store(TinyStore::default()).unwrap();
    run(&mut tiny, false, false).await;
    assert!(tiny.record_metadata(completion_id(&tiny)).is_none());
}
#[tokio::test]
async fn cancelled_and_incomplete_attempts_do_not_store_layout() {
    for (fail, incomplete) in [(true, false), (false, true)] {
        let mut session = Session::with_store(pi_rs_core::MemoryStore::default()).unwrap();
        run(&mut session, fail, incomplete).await;
        assert!(session.record_metadata(completion_id(&session)).is_none());
    }
}

#[tokio::test]
async fn completion_observer_receives_only_metadata_actually_persisted() {
    for supports in [true, false] {
        let model = Fixture {
            fail: false,
            incomplete: false,
        };
        let registry = Registry::default();
        let agent = Agent {
            model: &model,
            model_name: "fixture",
            registry: &registry,
            system_prompt: "",
            limits: Limits::default(),
        };
        let mut session = if supports {
            Session::with_store(pi_rs_core::MemoryStore::default()).unwrap()
        } else {
            Session::with_store(TinyStore::default()).unwrap()
        };
        let (_sender, mut inputs) = tokio::sync::mpsc::channel(1);
        let mut observed = None;
        agent
            .run(
                &mut session,
                Some("hello".into()),
                &mut inputs,
                &CancellationToken::new(),
                &mut |event| {
                    if let pi_rs_core::Notification::Committed(record) = event
                        && matches!(record.event, Event::ModelFinished { .. })
                    {
                        observed = Some(pi_rs_core::record_event_metadata(record.id));
                    }
                },
            )
            .await
            .unwrap();
        assert_eq!(
            observed.expect("completion notification").is_some(),
            supports
        );
        assert!(pi_rs_core::record_event_metadata(completion_id(&session)).is_none());
    }
}
