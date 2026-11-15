use async_trait::async_trait;
use futures_util::{StreamExt, stream};
use pi_rs_core::{CancellationToken, Error, Model, ModelEvent, ModelRequest, ModelStream};
use pi_rs_durable::{
    images::{ImageLimits, ImageProcessor, ModelImageProfile, PreparedImage, UserImageAdmission},
    model::{DurableRequest, ProviderAdapter, ProviderAttempt},
    recovery::UncertainPolicy,
    runtime::{Config, Harness, Options},
};
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
use uuid::Uuid;

struct Unused;
#[async_trait]
impl Model for Unused {
    async fn stream(&self, _: ModelRequest) -> Result<ModelStream, Error> {
        panic!("unexpected core model")
    }
}
#[derive(Default)]
struct Adapter(Mutex<Vec<Value>>);
#[async_trait]
impl ProviderAdapter for Adapter {
    fn id(&self) -> &str {
        "image-admission-test/v1"
    }
    fn supports_durable_blocks(&self) -> bool {
        true
    }
    fn image_profile(&self, model: &str) -> Option<ModelImageProfile> {
        Some(ModelImageProfile {
            supports_images: true,
            limits: ImageLimits {
                max_width: if model == "second" { 25 } else { 12 },
                ..Default::default()
            },
        })
    }
    async fn prepare(&self, _: &ProviderAttempt, _: &ModelRequest) -> Result<Value, Error> {
        Ok(json!({}))
    }
    fn validate(&self, _: &ProviderAttempt, _: &ModelRequest, _: &Value) -> Result<(), Error> {
        Ok(())
    }
    async fn prepare_durable(
        &self,
        _: &ProviderAttempt,
        req: &DurableRequest,
    ) -> Result<Value, Error> {
        Ok(json!({"blocks": req.blocks}))
    }
    fn validate_durable(
        &self,
        _: &ProviderAttempt,
        req: &DurableRequest,
        body: &Value,
    ) -> Result<(), Error> {
        if body["blocks"] != json!(req.blocks) {
            return Err(Error::Config("blocks changed".into()));
        }
        Ok(())
    }
    async fn dispatch(
        &self,
        _: &ProviderAttempt,
        _: &ModelRequest,
        body: Value,
    ) -> Result<ModelStream, Error> {
        self.0.lock().unwrap().push(body);
        Ok(stream::iter(vec![
            Ok(ModelEvent::Text("done".into())),
            Ok(ModelEvent::Complete {
                calls: vec![],
                usage: None,
            }),
        ])
        .boxed())
    }
}
#[derive(Default)]
struct Processor(Mutex<Vec<u32>>);
#[async_trait]
impl ImageProcessor for Processor {
    async fn prepare(
        &self,
        bytes: &[u8],
        _: &str,
        limits: ImageLimits,
    ) -> Result<Option<PreparedImage>, String> {
        self.0.lock().unwrap().push(limits.max_width);
        if bytes == b"bad" {
            return Err("broken image".into());
        }
        Ok(Some(PreparedImage {
            data: "bmV3".into(),
            mime_type: "image/png".into(),
            resized: None,
            converted_from: None,
        }))
    }
}
fn options() -> Options {
    Options {
        model: Arc::new(Unused),
        tools: vec![],
        restart_limit: 3,
        max_turns: 3,
        uncertain_policy: UncertainPolicy::InterruptedResult,
    }
}
fn config(model: &str) -> Config {
    Config {
        model: model.into(),
        system_prompt: String::new(),
        max_output_tokens: 100,
    }
}
struct Switch;
impl pi_rs_durable::task::scheduler::Handler for Switch {
    fn run(
        &self,
        _: &pi_rs_durable::task::Task,
    ) -> Result<pi_rs_durable::task::scheduler::Step, String> {
        unreachable!()
    }
    fn run_with_documents(
        &self,
        _: &pi_rs_durable::task::Task,
        docs: &mut pi_rs_durable::document::TaskDocuments<'_>,
    ) -> Result<pi_rs_durable::task::scheduler::Step, String> {
        docs.append_entry("model.switch", json!(null)).unwrap();
        docs.configure_runtime_at_checkpoint(config("second"))
            .unwrap();
        Ok(pi_rs_durable::task::scheduler::Step::Succeed(json!(null)))
    }
}
#[tokio::test]
async fn fit_admission_is_atomic_reopens_and_uses_current_model() {
    use pi_rs_durable::task::{Checkpoint, Task, TaskOwner, scheduler::Registry};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let adapter = Arc::new(Adapter::default());
    let processor = Arc::new(Processor::default());
    let fit = UserImageAdmission::new(processor.clone());
    let mut h = Harness::open_paused(&path, options()).unwrap();
    h.install_provider_adapter(adapter.clone()).unwrap();
    let id = h.create_conversation(config("first")).unwrap();
    h.select_provider_adapter(id, true).unwrap();
    let input = vec![
        json!({"type":"text","text":"look"}),
        json!({"type":"image","data":"aGk=","mimeType":"image/png"}),
        json!({"type":"image","data":"YmFk","mimeType":"image/png"}),
    ];
    let result = h
        .submit_fitted_user_blocks(
            id,
            Some("req".into()),
            input.clone(),
            &fit,
            &CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(result.notes.len(), 1);
    assert!(result.notes[0].contains("broken image"));
    let before = h.conversation(id).unwrap();
    let binding = before.content_blocks.values().next().unwrap();
    assert_eq!(binding.blocks[1]["data"], "bmV3");
    assert!(
        binding.blocks[2]["text"]
            .as_str()
            .unwrap()
            .contains("broken image")
    );
    assert!(
        !serde_json::to_string(&binding.blocks)
            .unwrap()
            .contains("YmFk")
    );
    drop(h);
    let mut h = Harness::open_paused(&path, options()).unwrap();
    h.install_provider_adapter(adapter.clone()).unwrap();
    let retry = h
        .submit_fitted_user_blocks(
            id,
            Some("req".into()),
            input.clone(),
            &fit,
            &CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(retry, result);
    assert_eq!(processor.0.lock().unwrap().len(), 2);
    h.resume_conversation(id, &CancellationToken::new())
        .await
        .unwrap();
    assert!(adapter.0.lock().unwrap()[0].to_string().contains("bmV3"));
    let fork = h.fork(id).unwrap();
    let mut registry = Registry::default();
    registry.register("switch", true, Switch);
    let mut scheduler = h.attach_tasks(registry).unwrap();
    let task = Task::new(
        Uuid::new_v4(),
        TaskOwner::Conversation(fork),
        Checkpoint {
            phase: "start".into(),
            data: json!(null),
        },
        3,
    )
    .unwrap()
    .registered("switch".into(), true);
    scheduler.submit(&task).unwrap();
    scheduler.tick(0).unwrap();
    drop(scheduler);
    let result2 = h
        .submit_fitted_user_blocks(
            fork,
            None,
            vec![input[1].clone()],
            &fit,
            &CancellationToken::new(),
        )
        .await
        .unwrap();
    assert!(result2.notes.is_empty());
    assert_eq!(*processor.0.lock().unwrap(), vec![12, 12, 25]);
    h.resume_conversation(fork, &CancellationToken::new())
        .await
        .unwrap();
    let forked = h.conversation(fork).unwrap();
    assert!(
        forked
            .content_blocks
            .values()
            .any(|b| b.blocks.iter().any(|v| v["data"] == "bmV3"))
    );
    drop(h);
    let h = Harness::open_paused(&path, options()).unwrap();
    assert_eq!(
        h.conversation(fork).unwrap().content_blocks,
        forked.content_blocks
    );
}
#[tokio::test]
async fn cancelled_or_rejected_admission_saves_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = Harness::open_paused(&path, options()).unwrap();
    let adapter = Arc::new(Adapter::default());
    h.install_provider_adapter(adapter).unwrap();
    let id = h.create_conversation(config("first")).unwrap();
    h.select_provider_adapter(id, true).unwrap();
    let fit = UserImageAdmission::new(Arc::new(Processor::default()));
    let cancel = CancellationToken::new();
    cancel.cancel();
    assert!(
        h.submit_fitted_user_blocks(
            id,
            None,
            vec![json!({"type":"image","data":"aGk=","mimeType":"image/png"})],
            &fit,
            &cancel
        )
        .await
        .is_err()
    );
    assert!(
        h.submit_fitted_user_blocks(
            id,
            None,
            vec![json!({"type":"bad"})],
            &fit,
            &CancellationToken::new()
        )
        .await
        .is_err()
    );
    assert!(h.conversation(id).unwrap().submissions.is_empty());
    assert!(h.conversation(id).unwrap().content_blocks.is_empty());
}

struct GateProcessor {
    ready: Mutex<Option<std::sync::mpsc::Sender<()>>>,
    wake: Arc<tokio::sync::Notify>,
}
#[async_trait]
impl ImageProcessor for GateProcessor {
    async fn prepare(
        &self,
        _: &[u8],
        _: &str,
        _: ImageLimits,
    ) -> Result<Option<PreparedImage>, String> {
        self.ready.lock().unwrap().take().unwrap().send(()).unwrap();
        self.wake.notified().await;
        Ok(Some(PreparedImage {
            data: "bmV3".into(),
            mime_type: "image/png".into(),
            resized: None,
            converted_from: None,
        }))
    }
}
#[tokio::test]
async fn concurrent_model_switch_fences_inflight_fit() {
    use pi_rs_durable::task::{Checkpoint, Task, TaskOwner, scheduler::Registry};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = Harness::open_paused(&path, options()).unwrap();
    h.install_provider_adapter(Arc::new(Adapter::default()))
        .unwrap();
    let id = h.create_conversation(config("first")).unwrap();
    h.select_provider_adapter(id, true).unwrap();
    let mut registry = Registry::default();
    registry.register("switch", true, Switch);
    let mut scheduler = h.attach_tasks(registry).unwrap();
    let task = Task::new(
        Uuid::new_v4(),
        TaskOwner::Conversation(id),
        Checkpoint {
            phase: "start".into(),
            data: json!(null),
        },
        3,
    )
    .unwrap()
    .registered("switch".into(), true);
    scheduler.submit(&task).unwrap();
    let (ready, started) = std::sync::mpsc::channel();
    let wake = Arc::new(tokio::sync::Notify::new());
    let fit = UserImageAdmission::new(Arc::new(GateProcessor {
        ready: Mutex::new(Some(ready)),
        wake: wake.clone(),
    }));
    let worker = std::thread::spawn(move || {
        started.recv().unwrap();
        scheduler.tick(0).unwrap();
        wake.notify_one();
    });
    let result = h
        .submit_fitted_user_blocks(
            id,
            Some("stale".into()),
            vec![json!({"type":"image","data":"aGk=","mimeType":"image/png"})],
            &fit,
            &CancellationToken::new(),
        )
        .await;
    worker.join().unwrap();
    assert!(result.is_err());
    let c = h.conversation(id).unwrap();
    assert_eq!(c.config.model, "second");
    assert!(c.submissions.is_empty());
    assert!(c.content_blocks.is_empty());
}

#[tokio::test]
async fn scheduler_runs_only_after_fitted_admission() {
    let dir = tempfile::tempdir().unwrap();
    let adapter = Arc::new(Adapter::default());
    let mut scheduler = pi_rs_durable::runtime::Scheduler::open_with_provider_adapter(
        dir.path().join("db"),
        options(),
        Default::default(),
        adapter.clone(),
    )
    .unwrap();
    let id = scheduler.create_conversation(config("first")).unwrap();
    scheduler.select_provider_adapter(id, true).unwrap();
    let fit = UserImageAdmission::new(Arc::new(Processor::default()));
    scheduler
        .submit_fitted_user_blocks(
            id,
            None,
            vec![json!({"type":"image","data":"aGk=","mimeType":"image/png"})],
            &fit,
            &CancellationToken::new(),
        )
        .await
        .unwrap();
    scheduler.wait(id).await.unwrap();
    assert!(adapter.0.lock().unwrap()[0].to_string().contains("bmV3"));
    scheduler.shutdown().await.unwrap();
}

struct GatedAdapter {
    inner: Adapter,
    started: tokio::sync::Semaphore,
    release: tokio::sync::Semaphore,
    first: std::sync::atomic::AtomicBool,
}
impl GatedAdapter {
    fn new() -> Self {
        Self {
            inner: Adapter::default(),
            started: tokio::sync::Semaphore::new(0),
            release: tokio::sync::Semaphore::new(0),
            first: std::sync::atomic::AtomicBool::new(true),
        }
    }
}
#[async_trait]
impl ProviderAdapter for GatedAdapter {
    fn id(&self) -> &str {
        self.inner.id()
    }
    fn supports_durable_blocks(&self) -> bool {
        true
    }
    fn image_profile(&self, model: &str) -> Option<ModelImageProfile> {
        self.inner.image_profile(model)
    }
    async fn prepare(&self, a: &ProviderAttempt, r: &ModelRequest) -> Result<Value, Error> {
        self.inner.prepare(a, r).await
    }
    fn validate(&self, a: &ProviderAttempt, r: &ModelRequest, v: &Value) -> Result<(), Error> {
        self.inner.validate(a, r, v)
    }
    async fn prepare_durable(
        &self,
        a: &ProviderAttempt,
        r: &DurableRequest,
    ) -> Result<Value, Error> {
        self.inner.prepare_durable(a, r).await
    }
    fn validate_durable(
        &self,
        a: &ProviderAttempt,
        r: &DurableRequest,
        v: &Value,
    ) -> Result<(), Error> {
        self.inner.validate_durable(a, r, v)
    }
    async fn dispatch(
        &self,
        a: &ProviderAttempt,
        r: &ModelRequest,
        v: Value,
    ) -> Result<ModelStream, Error> {
        if self.first.swap(false, std::sync::atomic::Ordering::SeqCst) {
            self.started.add_permits(1);
            self.release.acquire().await.unwrap().forget();
        }
        self.inner.dispatch(a, r, v).await
    }
}
fn image_block() -> Value {
    json!({"type":"image","data":"aGk=","mimeType":"image/png"})
}

#[tokio::test]
async fn busy_fitted_inputs_queue_dedupe_and_survive_reopen() {
    use pi_rs_durable::runtime::{AdmissionOptions, InputMode, Scheduler};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let adapter = Arc::new(GatedAdapter::new());
    let mut s = Scheduler::open_with_provider_adapter(
        &path,
        options(),
        Default::default(),
        adapter.clone(),
    )
    .unwrap();
    let id = s.create_conversation(config("first")).unwrap();
    s.select_provider_adapter(id, true).unwrap();
    s.submit(id, None, "start".into()).unwrap();
    adapter.started.acquire().await.unwrap().forget();
    let proc = Arc::new(Processor::default());
    let fit = UserImageAdmission::new(proc.clone());
    let steer = s
        .submit_fitted_user_blocks_with_options(
            id,
            Some("steer".into()),
            vec![json!({"type":"text","text":"steer"}), image_block()],
            &fit,
            AdmissionOptions {
                mode: InputMode::Steer,
                ..Default::default()
            },
            &CancellationToken::new(),
        )
        .await
        .unwrap();
    let duplicate = s
        .submit_fitted_user_blocks_with_options(
            id,
            Some("steer".into()),
            vec![image_block()],
            &fit,
            AdmissionOptions::default(),
            &CancellationToken::new(),
        )
        .await
        .unwrap();
    assert_eq!(steer, duplicate);
    s.submit_fitted_user_blocks(
        id,
        Some("follow".into()),
        vec![json!({"type":"text","text":"follow"}), image_block()],
        &fit,
        &CancellationToken::new(),
    )
    .await
    .unwrap();
    assert_eq!(proc.0.lock().unwrap().len(), 2);
    assert_eq!(
        s.conversation(id)
            .unwrap()
            .submissions
            .iter()
            .filter(|x| matches!(x.status, pi_rs_durable::runtime::Status::Queued))
            .count(),
        2
    );
    adapter.release.add_permits(1);
    s.wait(id).await.unwrap();
    let state = s.conversation(id).unwrap();
    let bindings: Vec<_> = state
        .content_blocks
        .values()
        .map(|b| b.blocks[0]["text"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(bindings, ["steer", "follow"]);
    let expected = state.content_blocks.clone();
    s.shutdown().await.unwrap();
    let h = Harness::open_paused(&path, options()).unwrap();
    assert_eq!(h.conversation(id).unwrap().content_blocks, expected);
}

struct ContinueFit {
    admission: UserImageAdmission,
    fired: std::sync::atomic::AtomicBool,
}
#[async_trait]
impl pi_rs_durable::extension::Hook for ContinueFit {
    async fn generation(
        &self,
        event: pi_rs_durable::extension::GenerationEvent,
        _: &Value,
        ctx: &pi_rs_durable::extension::Context<'_>,
    ) -> Result<pi_rs_durable::extension::GenerationDecision, String> {
        use pi_rs_durable::extension::{GenerationDecision, GenerationEvent};
        if event == GenerationEvent::OnYield
            && !self.fired.swap(true, std::sync::atomic::Ordering::SeqCst)
        {
            let (blocks, _) = self
                .admission
                .prepare_for_context(&[image_block()], ctx)
                .await?;
            return Ok(GenerationDecision::Continue(json!(blocks)));
        }
        Ok(GenerationDecision::Pass)
    }
}
#[tokio::test]
async fn host_yield_continuation_fits_before_persistence() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let adapter = Arc::new(Adapter::default());
    let proc = Arc::new(Processor::default());
    let mut h = Harness::open_paused(&path, options()).unwrap();
    h.install_provider_adapter(adapter.clone()).unwrap();
    h.install_extension(pi_rs_durable::extension::Extension {
        name: "continue-fit".into(),
        hooks: vec![Arc::new(ContinueFit {
            admission: UserImageAdmission::new(proc.clone()),
            fired: false.into(),
        })],
        ..Default::default()
    })
    .unwrap();
    let id = h.create_conversation(config("second")).unwrap();
    h.select_provider_adapter(id, true).unwrap();
    h.select_extensions(id, vec!["continue-fit".into()])
        .unwrap();
    h.submit_and_run(id, None, "start".into(), &CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(*proc.0.lock().unwrap(), vec![25]);
    let state = h.conversation(id).unwrap();
    assert!(
        state
            .content_blocks
            .values()
            .any(|b| b.blocks[0]["data"] == "bmV3")
    );
    let expected = state.content_blocks;
    drop(h);
    assert_eq!(
        Harness::open_paused(&path, options())
            .unwrap()
            .conversation(id)
            .unwrap()
            .content_blocks,
        expected
    );
}

#[tokio::test]
async fn busy_fitted_reset_precedes_queued_fitted_followup_and_reopens() {
    use pi_rs_durable::runtime::Scheduler;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let adapter = Arc::new(GatedAdapter::new());
    let mut s = Scheduler::open_with_provider_adapter(
        &path,
        options(),
        Default::default(),
        adapter.clone(),
    )
    .unwrap();
    let id = s.create_conversation(config("first")).unwrap();
    s.select_provider_adapter(id, true).unwrap();
    s.submit(id, None, "start".into()).unwrap();
    adapter.started.acquire().await.unwrap().forget();
    let fit = UserImageAdmission::new(Arc::new(Processor::default()));
    let reset = s
        .reset_with_fitted_blocks(
            id,
            Some("reset-key".into()),
            Some("handoff".into()),
            vec![vec![json!({"type":"text","text":"reset"}), image_block()]],
            &fit,
            &CancellationToken::new(),
        )
        .await
        .unwrap();
    let reset_ids = reset.1.clone();
    assert_eq!(
        s.reset_with_fitted_blocks(
            id,
            Some("reset-key".into()),
            None,
            vec![vec![image_block()]],
            &fit,
            &CancellationToken::new()
        )
        .await
        .unwrap(),
        reset
    );
    s.submit_fitted_user_blocks(
        id,
        Some("after".into()),
        vec![json!({"type":"text","text":"after"}), image_block()],
        &fit,
        &CancellationToken::new(),
    )
    .await
    .unwrap();
    adapter.release.add_permits(1);
    s.wait(id).await.unwrap();
    let c = s.conversation(id).unwrap();
    let bindings: Vec<_> = c.content_blocks.values().collect();
    assert_eq!(bindings.len(), 2);
    assert_eq!(bindings[0].message_id, reset_ids[0]);
    assert_eq!(bindings[0].blocks[0]["text"], "reset");
    assert_eq!(bindings[1].blocks[0]["text"], "after");
    let expected = c.content_blocks.clone();
    s.shutdown().await.unwrap();
    let mut h = Harness::open_paused(&path, options()).unwrap();
    assert_eq!(h.conversation(id).unwrap().content_blocks, expected);
    assert_eq!(
        h.reset_with_fitted_blocks(
            id,
            Some("reset-key".into()),
            None,
            vec![vec![image_block()]],
            &fit,
            &CancellationToken::new()
        )
        .await
        .unwrap(),
        reset
    );
}

#[tokio::test]
async fn abort_withdraws_queued_fitted_input_but_retains_fitted_reset() {
    use pi_rs_durable::runtime::{Scheduler, Status};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let adapter = Arc::new(GatedAdapter::new());
    let mut s = Scheduler::open_with_provider_adapter(
        &path,
        options(),
        Default::default(),
        adapter.clone(),
    )
    .unwrap();
    let id = s.create_conversation(config("first")).unwrap();
    s.select_provider_adapter(id, true).unwrap();
    s.submit(id, None, "start".into()).unwrap();
    adapter.started.acquire().await.unwrap().forget();
    let fit = UserImageAdmission::new(Arc::new(Processor::default()));
    let queued = s
        .submit_fitted_user_blocks(
            id,
            Some("queued".into()),
            vec![image_block()],
            &fit,
            &CancellationToken::new(),
        )
        .await
        .unwrap();
    let (_, ids, _) = s
        .reset_with_fitted_blocks(
            id,
            Some("reset-key".into()),
            None,
            vec![vec![image_block()]],
            &fit,
            &CancellationToken::new(),
        )
        .await
        .unwrap();
    s.abort(id).unwrap();
    s.wait(id).await.unwrap();
    let c = s.conversation(id).unwrap();
    let item = c
        .submissions
        .iter()
        .find(|x| x.id == queued.submission_id)
        .unwrap();
    assert_eq!(
        item.status,
        Status::Unanswered {
            reason: "aborted".into()
        }
    );
    assert!(c.content_blocks.values().any(|b| b.message_id == ids[0]));
    assert!(
        !c.content_blocks
            .values()
            .any(|b| Some(b) == item.input_blocks.as_ref())
    );
    let blocks = c.content_blocks.clone();
    s.shutdown().await.unwrap();
    let mut h = Harness::open_paused(&path, options()).unwrap();
    h.install_provider_adapter(adapter).unwrap();
    let c = h.conversation(id).unwrap();
    assert_eq!(c.content_blocks, blocks);
    assert_eq!(
        h.submit_fitted_user_blocks(
            id,
            Some("queued".into()),
            vec![image_block()],
            &fit,
            &CancellationToken::new()
        )
        .await
        .unwrap(),
        queued
    );
}

#[tokio::test]
async fn queued_fitted_input_survives_scheduler_shutdown_and_recovery() {
    use pi_rs_durable::runtime::{Scheduler, Status};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let adapter = Arc::new(GatedAdapter::new());
    let mut s = Scheduler::open_with_provider_adapter(
        &path,
        options(),
        Default::default(),
        adapter.clone(),
    )
    .unwrap();
    let id = s.create_conversation(config("first")).unwrap();
    s.select_provider_adapter(id, true).unwrap();
    s.submit(id, None, "start".into()).unwrap();
    adapter.started.acquire().await.unwrap().forget();
    let proc = Arc::new(Processor::default());
    let fit = UserImageAdmission::new(proc.clone());
    let queued = s
        .submit_fitted_user_blocks(
            id,
            Some("recover".into()),
            vec![image_block()],
            &fit,
            &CancellationToken::new(),
        )
        .await
        .unwrap();
    s.shutdown().await.unwrap();
    let mut h = Harness::open_paused(&path, options()).unwrap();
    h.install_provider_adapter(adapter.clone()).unwrap();
    let state = h.conversation(id).unwrap();
    let q = state
        .submissions
        .iter()
        .find(|x| x.id == queued.submission_id)
        .unwrap();
    assert_eq!(q.status, Status::Queued);
    assert_eq!(q.input_blocks.as_ref().unwrap().blocks[0]["data"], "bmV3");
    assert_eq!(
        h.submit_fitted_user_blocks(
            id,
            Some("recover".into()),
            vec![image_block()],
            &fit,
            &CancellationToken::new()
        )
        .await
        .unwrap(),
        queued
    );
    assert_eq!(proc.0.lock().unwrap().len(), 1);
    h.resume_conversation(id, &CancellationToken::new())
        .await
        .unwrap();
    assert!(
        h.conversation(id)
            .unwrap()
            .content_blocks
            .values()
            .any(|b| b.blocks[0]["data"] == "bmV3")
    );
}
