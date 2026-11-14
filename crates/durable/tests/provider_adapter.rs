use async_trait::async_trait;
use futures_util::{StreamExt, stream};
use pi_rs_core::{CancellationToken, Error, Model, ModelEvent, ModelRequest, ModelStream};
use pi_rs_durable::{
    extension::{Context, Extension, GenerationDecision, GenerationEvent, Hook},
    model::{ProviderAdapter, ProviderAttempt},
    recovery::UncertainPolicy,
    runtime::{Config, Harness, Options, Status},
};
use serde_json::{Value, json};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

#[derive(Default)]
struct Regular(AtomicUsize);
#[async_trait]
impl Model for Regular {
    async fn stream(&self, _: ModelRequest) -> Result<ModelStream, Error> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(answer())
    }
}
fn answer() -> ModelStream {
    stream::iter(vec![
        Ok(ModelEvent::Text("ok".into())),
        Ok(ModelEvent::Complete {
            calls: vec![],
            usage: None,
        }),
    ])
    .boxed()
}
#[derive(Default)]
struct Fake {
    seen: Mutex<Vec<(ProviderAttempt, Value)>>,
    retry: bool,
}
#[async_trait]
impl ProviderAdapter for Fake {
    fn id(&self) -> &str {
        "fake/v1"
    }
    async fn prepare(&self, _: &ProviderAttempt, request: &ModelRequest) -> Result<Value, Error> {
        Ok(json!({"model":request.model,"order":""}))
    }
    fn validate(
        &self,
        _: &ProviderAttempt,
        request: &ModelRequest,
        body: &Value,
    ) -> Result<(), Error> {
        if body["model"] != request.model {
            return Err(Error::Config("model changed".into()));
        }
        Ok(())
    }
    async fn dispatch(
        &self,
        attempt: &ProviderAttempt,
        _: &ModelRequest,
        body: Value,
    ) -> Result<ModelStream, Error> {
        let mut seen = self.seen.lock().unwrap();
        seen.push((attempt.clone(), body));
        if self.retry && seen.len() == 1 {
            return Err(Error::RetryableModel("temporary".into()));
        }
        Ok(answer())
    }
}
struct Rewrite(&'static str);
#[async_trait]
impl Hook for Rewrite {
    async fn generation(
        &self,
        event: GenerationEvent,
        body: &Value,
        _: &Context<'_>,
    ) -> Result<GenerationDecision, String> {
        if event != GenerationEvent::BeforeProviderRequest {
            return Ok(GenerationDecision::Pass);
        }
        match self.0 {
            "fail" => Err("denied".into()),
            "invalid" => Ok(GenerationDecision::Replace(json!({"payload":[]}))),
            "model" => Ok(GenerationDecision::Replace(
                json!({"payload":{"model":"other"}}),
            )),
            x => {
                let mut body = body["payload"].clone();
                body["order"] = json!(format!("{}{x}", body["order"].as_str().unwrap()));
                Ok(GenerationDecision::Replace(json!({"payload":body})))
            }
        }
    }
}
fn options(model: Arc<Regular>) -> Options {
    Options {
        model,
        tools: vec![],
        restart_limit: 3,
        max_turns: 3,
        uncertain_policy: UncertainPolicy::InterruptedResult,
    }
}
fn config() -> Config {
    Config {
        model: "fake".into(),
        system_prompt: "system".into(),
        max_output_tokens: 10,
    }
}
fn install(h: &mut Harness, fake: Arc<Fake>, hooks: &[&'static str]) {
    h.install_provider_adapter(fake).unwrap();
    for (i, hook) in hooks.iter().enumerate() {
        h.install_extension(Extension {
            name: format!("e{i}"),
            hooks: vec![Arc::new(Rewrite(hook))],
            ..Default::default()
        })
        .unwrap();
    }
}
fn select(h: &mut Harness, hooks: usize) -> uuid::Uuid {
    let id = h.create_conversation(config()).unwrap();
    h.select_provider_adapter(id, true).unwrap();
    h.select_extensions(id, (0..hooks).map(|i| format!("e{i}")).collect())
        .unwrap();
    id
}
#[tokio::test]
async fn scheduler_can_restore_provider_and_subagent_management_together() {
    use pi_rs_durable::{extension::Registry, runtime::Scheduler, subagent_management};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let model = Arc::new(Regular::default());
    let fake = Arc::new(Fake::default());
    let id;
    {
        let mut h = Harness::open_paused(&path, options(model.clone())).unwrap();
        install(&mut h, fake.clone(), &[]);
        id = select(&mut h, 0);
    }
    let mut scheduler = Scheduler::open_with_provider_and_subagent_management(
        &path,
        options(model.clone()),
        Registry::default(),
        fake.clone(),
        id,
        config(),
        Some(vec![]),
    )
    .unwrap();
    scheduler
        .select_extensions(id, vec![subagent_management::extension_name(id)])
        .unwrap();
    scheduler.submit(id, None, "hello".into()).unwrap();
    scheduler.wait(id).await.unwrap();
    assert_eq!(fake.seen.lock().unwrap().len(), 1);
    assert_eq!(model.0.load(Ordering::SeqCst), 0);
    scheduler.shutdown().await.unwrap();
}

#[tokio::test]
async fn historical_fork_preserves_adapter_and_tool_filter() {
    use pi_rs_durable::observer::Cursor;
    let dir = tempfile::tempdir().unwrap();
    let mut h =
        Harness::open_paused(dir.path().join("db"), options(Arc::new(Regular::default()))).unwrap();
    install(&mut h, Arc::new(Fake::default()), &[]);
    let id = select(&mut h, 0);
    h.select_tools(id, Some(vec![])).unwrap();
    let cursor = h
        .history(id, Cursor(0), std::num::NonZeroUsize::new(100).unwrap())
        .unwrap()
        .next;
    h.select_tools(id, None).unwrap();
    h.select_provider_adapter(id, false).unwrap();
    let child = h.fork_at(id, cursor).unwrap();
    let state = h.conversation(child).unwrap();
    assert_eq!(state.provider_adapter.as_deref(), Some("fake/v1"));
    assert_eq!(state.allowed_tools, Some(vec![]));
}

#[tokio::test]
async fn scheduler_preloads_adapter_before_recovery() {
    use pi_rs_durable::{extension::Registry, runtime::Scheduler};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let model = Arc::new(Regular::default());
    let fake = Arc::new(Fake::default());
    let id;
    {
        let mut h = Harness::open_paused(&path, options(model.clone())).unwrap();
        install(&mut h, fake.clone(), &["a"]);
        id = select(&mut h, 1);
        h.submit(id, None, "hello".into()).unwrap();
    }
    let mut registry = Registry::default();
    registry
        .install(Extension {
            name: "e0".into(),
            hooks: vec![Arc::new(Rewrite("a"))],
            ..Default::default()
        })
        .unwrap();
    let mut scheduler = Scheduler::open_with_provider_adapter(
        &path,
        options(model.clone()),
        registry,
        fake.clone(),
    )
    .unwrap();
    scheduler.wait(id).await.unwrap();
    assert!(matches!(
        scheduler.conversation(id).unwrap().submissions[0].status,
        Status::Done { .. }
    ));
    assert_eq!(fake.seen.lock().unwrap()[0].1["order"], "a");
    assert_eq!(model.0.load(Ordering::SeqCst), 0);
    scheduler.shutdown().await.unwrap();
}

#[tokio::test]
async fn transforms_retry_and_regular_route() {
    let d = tempfile::tempdir().unwrap();
    let model = Arc::new(Regular::default());
    let fake = Arc::new(Fake {
        retry: true,
        ..Default::default()
    });
    let mut h = Harness::open_paused(d.path().join("db"), options(model.clone())).unwrap();
    install(&mut h, fake.clone(), &["a", "b"]);
    let id = select(&mut h, 2);
    h.submit_and_run(id, None, "hi".into(), &CancellationToken::new())
        .await
        .unwrap();
    {
        let seen = fake.seen.lock().unwrap();
        assert_eq!(seen.len(), 2);
        assert_eq!(seen[0].1["order"], "ab");
        assert_eq!(seen[1].1["order"], "ab");
        assert_eq!(seen[0].0.session_id, id);
        assert_ne!(seen[0].0.request_id, seen[1].0.request_id);
        assert_eq!((seen[0].0.retries, seen[1].0.retries), (0, 1));
    }
    let regular = h.create_conversation(config()).unwrap();
    h.submit_and_run(regular, None, "hi".into(), &CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(model.0.load(Ordering::SeqCst), 1);
    assert!(matches!(
        h.conversation(regular).unwrap().submissions[0].status,
        Status::Done { .. }
    ));
}
#[tokio::test]
async fn failures_never_dispatch() {
    for kind in ["fail", "invalid", "model"] {
        let d = tempfile::tempdir().unwrap();
        let fake = Arc::new(Fake::default());
        let mut h =
            Harness::open_paused(d.path().join("db"), options(Arc::new(Regular::default())))
                .unwrap();
        install(&mut h, fake.clone(), &[kind]);
        let id = select(&mut h, 1);
        assert!(
            h.submit_and_run(id, None, "hi".into(), &CancellationToken::new())
                .await
                .is_err()
        );
        assert!(fake.seen.lock().unwrap().is_empty());
    }
}
#[tokio::test]
async fn reopen_requires_adapter_and_cancellation_does_not_dispatch() {
    let d = tempfile::tempdir().unwrap();
    let model = Arc::new(Regular::default());
    let fake = Arc::new(Fake::default());
    let id;
    {
        let mut h = Harness::open_paused(d.path().join("db"), options(model.clone())).unwrap();
        install(&mut h, fake.clone(), &[]);
        id = select(&mut h, 0);
        h.submit(id, None, "hi".into()).unwrap();
    }
    {
        let mut h = Harness::open_paused(d.path().join("db"), options(model.clone())).unwrap();
        assert!(h.resume(&CancellationToken::new()).await.is_err());
        assert!(fake.seen.lock().unwrap().is_empty());
    }
    {
        let mut h = Harness::open_paused(d.path().join("db"), options(model.clone())).unwrap();
        install(&mut h, fake.clone(), &[]);
        let cancel = CancellationToken::new();
        cancel.cancel();
        assert!(h.resume(&cancel).await.is_err());
        assert!(fake.seen.lock().unwrap().is_empty());
    }
    let mut h = Harness::open_paused(d.path().join("db"), options(model)).unwrap();
    install(&mut h, fake.clone(), &[]);
    h.resume(&CancellationToken::new()).await.unwrap();
    let seen = fake.seen.lock().unwrap();
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].0.session_id, id);
    assert!(seen[0].0.restarts >= 1);
}

struct WaitingHook(Arc<tokio::sync::Notify>);
#[async_trait]
impl Hook for WaitingHook {
    async fn generation(
        &self,
        event: GenerationEvent,
        _: &Value,
        _: &Context<'_>,
    ) -> Result<GenerationDecision, String> {
        if event == GenerationEvent::BeforeProviderRequest {
            self.0.notify_one();
            std::future::pending::<()>().await;
        }
        Ok(GenerationDecision::Pass)
    }
}
#[tokio::test]
async fn cancellation_during_hook_stops_dispatch_and_recovers_with_new_attempt() {
    let d = tempfile::tempdir().unwrap();
    let model = Arc::new(Regular::default());
    let fake = Arc::new(Fake::default());
    let notify = Arc::new(tokio::sync::Notify::new());
    let id;
    {
        let mut h = Harness::open_paused(d.path().join("db"), options(model.clone())).unwrap();
        h.install_provider_adapter(fake.clone()).unwrap();
        h.install_extension(Extension {
            name: "wait".into(),
            hooks: vec![Arc::new(WaitingHook(notify.clone()))],
            ..Default::default()
        })
        .unwrap();
        id = h.create_conversation(config()).unwrap();
        h.select_provider_adapter(id, true).unwrap();
        h.select_extensions(id, vec!["wait".into()]).unwrap();
        h.submit(id, None, "hi".into()).unwrap();
        let cancel = CancellationToken::new();
        let stop = async {
            notify.notified().await;
            cancel.cancel();
        };
        let (result, ()) = tokio::join!(h.resume(&cancel), stop);
        assert!(result.is_err());
        assert!(fake.seen.lock().unwrap().is_empty());
    }
    let mut h = Harness::open_paused(d.path().join("db"), options(model)).unwrap();
    h.install_provider_adapter(fake.clone()).unwrap();
    h.install_extension(Extension {
        name: "wait".into(),
        hooks: vec![Arc::new(Rewrite("a"))],
        ..Default::default()
    })
    .unwrap();
    h.resume(&CancellationToken::new()).await.unwrap();
    let seen = fake.seen.lock().unwrap();
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].0.session_id, id);
    assert_eq!(seen[0].0.restarts, 1);
}
