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
