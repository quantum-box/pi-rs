use async_trait::async_trait;
use futures_util::{StreamExt, stream};
use pi_rs_core::{
    CancellationToken, Error, Message, Model, ModelEvent, ModelRequest, ModelStream, Outcome, Tool,
    ToolCall, ToolContext, ToolSpec,
};
use pi_rs_durable::{
    extension::{Context, Extension, Hook, ToolBlockResult, ToolCall as HookCall},
    recovery::{ReplayPolicy, UncertainPolicy},
    runtime::{Config, Harness, Options, RegisteredTool},
};
use serde_json::{Value, json};
use std::sync::Arc;
struct ModelFixture;
#[async_trait]
impl Model for ModelFixture {
    async fn stream(&self, _: ModelRequest) -> Result<ModelStream, Error> {
        Ok(stream::iter(vec![Ok(ModelEvent::Complete {
            calls: vec![ToolCall {
                id: "c".into(),
                name: "image".into(),
                arguments: "{}".into(),
            }],
            usage: None,
        })])
        .boxed())
    }
}
struct ToolFixture;
#[async_trait]
impl Tool for ToolFixture {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: "image".into(),
            description: String::new(),
            parameters: json!({"type":"object"}),
        }
    }
    async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
        Outcome::Structured {
            data: json!({"blocks":[{"type":"text","text":"before"}, {"type":"image","data":"aGk=","mimeType":"image/png"}, {"type":"text","text":"after"}]}),
            attachments: vec![],
        }
    }
}
struct Blocks;
#[async_trait]
impl Hook for Blocks {
    fn supports_compaction_blocks(&self) -> bool {
        true
    }
    fn supports_compaction_tool_blocks(&self) -> bool {
        true
    }
    fn supports_runtime_tool_blocks(&self) -> bool {
        true
    }
    async fn after_tool_blocks(
        &self,
        _: &HookCall,
        result: &ToolBlockResult,
        nested: bool,
        _: &Context<'_>,
    ) -> Result<Option<ToolBlockResult>, String> {
        assert!(!nested);
        let mut result = result.clone();
        result.output = result.extra["structuredOutput"]["blocks"]
            .as_array()
            .unwrap()
            .clone();
        Ok(Some(result))
    }
}
fn options() -> Options {
    Options {
        model: Arc::new(ModelFixture),
        tools: vec![RegisteredTool {
            tool: Arc::new(ToolFixture),
            replay: ReplayPolicy::Unsafe,
        }],
        restart_limit: 3,
        max_turns: 4,
        uncertain_policy: UncertainPolicy::InterruptedResult,
    }
}
fn setup(path: &std::path::Path) -> Harness {
    let h = Harness::open_paused(path, options()).unwrap();
    h.install_extension(Extension {
        name: "blocks".into(),
        hooks: vec![Arc::new(Blocks)],
        ..Default::default()
    })
    .unwrap();
    h
}
#[tokio::test]
async fn tool_block_child() {
    let Ok(path) = std::env::var("PI_TEST_TOOL_BLOCK_PATH") else {
        return;
    };
    let path = std::path::Path::new(&path);
    let mut h = setup(path);
    let id = h
        .create_conversation(Config {
            model: "fake".into(),
            system_prompt: String::new(),
            max_output_tokens: 100,
        })
        .unwrap();
    h.select_extensions(id, vec!["blocks".into()]).unwrap();
    assert!(
        h.submit_and_run(id, None, "go".into(), &CancellationToken::new())
            .await
            .is_err()
    );
    drop(h);
    let h = setup(path);
    let c = h.conversation(id).unwrap();
    assert!(
        c.content_blocks
            .iter()
            .any(|(i, _)| matches!(c.messages.get(*i), Some(Message::Tool { .. })))
    );
    std::fs::write(path.with_extension("id"), id.to_string()).unwrap();
    std::future::pending::<()>().await;
}
#[tokio::test]
async fn tool_block_survives_process_kill_and_fails_closed() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "tool_block_child", "--nocapture"])
        .env("PI_TEST_TOOL_BLOCK_PATH", &path)
        .spawn()
        .unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while !path.with_extension("id").exists() {
        assert!(
            child.try_wait().unwrap().is_none(),
            "child exited before saving blocks"
        );
        if std::time::Instant::now() >= deadline {
            let _ = child.kill();
            panic!("child did not save blocks");
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    child.kill().unwrap();
    child.wait().unwrap();
    let id = std::fs::read_to_string(path.with_extension("id"))
        .unwrap()
        .parse()
        .unwrap();
    let mut h = setup(&path);
    let c = h.conversation(id).unwrap();
    let (index, binding) = c
        .content_blocks
        .iter()
        .find(|(i, _)| matches!(c.messages.get(**i), Some(Message::Tool { .. })))
        .unwrap();
    assert!(
        matches!(&c.messages[*index], Message::Tool { call_id, content } if call_id == "c" && content.is_empty())
    );
    assert_eq!(
        binding.blocks,
        vec![
            json!({"type":"text","text":"before"}),
            json!({"type":"image","data":"aGk=","mimeType":"image/png"}),
            json!({"type":"text","text":"after"})
        ]
    );
    assert!(
        h.resume_conversation(id, &CancellationToken::new())
            .await
            .is_err()
    );
    assert!(h.request_compaction(id, 0).is_err());
    assert!(h.fork(id).is_err());
}

#[derive(Default)]
struct Adapter(std::sync::Mutex<Vec<Value>>);
#[async_trait]
impl pi_rs_durable::model::ProviderAdapter for Adapter {
    fn id(&self) -> &str {
        "tool-block-fixture/v1"
    }
    fn supports_durable_tool_blocks(&self) -> bool {
        true
    }
    async fn prepare(
        &self,
        _: &pi_rs_durable::model::ProviderAttempt,
        _: &ModelRequest,
    ) -> Result<Value, Error> {
        Ok(json!({"tool_blocks":{}}))
    }
    fn validate(
        &self,
        _: &pi_rs_durable::model::ProviderAttempt,
        _: &ModelRequest,
        _: &Value,
    ) -> Result<(), Error> {
        Ok(())
    }
    async fn prepare_with_tool_blocks(
        &self,
        _: &pi_rs_durable::model::ProviderAttempt,
        request: &pi_rs_durable::model::DurableToolRequest,
    ) -> Result<Value, Error> {
        request.validate_bindings()?;
        Ok(json!({"tool_blocks":request.tool_blocks}))
    }
    fn validate_with_tool_blocks(
        &self,
        _: &pi_rs_durable::model::ProviderAttempt,
        request: &pi_rs_durable::model::DurableToolRequest,
        body: &Value,
    ) -> Result<(), Error> {
        request.validate_bindings()?;
        if body["tool_blocks"] != json!(request.tool_blocks) {
            return Err(Error::Config("tool blocks changed".into()));
        }
        Ok(())
    }
    async fn dispatch(
        &self,
        _: &pi_rs_durable::model::ProviderAttempt,
        _: &ModelRequest,
        body: Value,
    ) -> Result<ModelStream, Error> {
        let is_result = !body["tool_blocks"].as_object().unwrap().is_empty();
        self.0.lock().unwrap().push(body);
        if is_result && let Ok(path) = std::env::var("PI_TEST_TOOL_BLOCK_PAUSE") {
            std::fs::write(path, b"ready").unwrap();
            std::future::pending::<()>().await;
        }
        let events = if is_result {
            vec![
                Ok(ModelEvent::Text("done".into())),
                Ok(ModelEvent::Complete {
                    calls: vec![],
                    usage: None,
                }),
            ]
        } else {
            vec![Ok(ModelEvent::Complete {
                calls: vec![ToolCall {
                    id: "c".into(),
                    name: "image".into(),
                    arguments: "{}".into(),
                }],
                usage: None,
            })]
        };
        Ok(stream::iter(events).boxed())
    }
}

struct Fit;
#[async_trait]
impl pi_rs_durable::images::ImageProcessor for Fit {
    async fn prepare(
        &self,
        _: &[u8],
        _: &str,
        _: pi_rs_durable::images::ImageLimits,
    ) -> Result<Option<pi_rs_durable::images::PreparedImage>, String> {
        Ok(Some(pi_rs_durable::images::PreparedImage {
            data: "bmV3".into(),
            mime_type: "image/png".into(),
            resized: None,
            converted_from: Some("image/bmp".into()),
        }))
    }
}

#[tokio::test]
async fn tool_blocks_reach_opt_in_adapter_and_survive_fork() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = setup(&path);
    let adapter = Arc::new(Adapter::default());
    h.install_provider_adapter(adapter.clone()).unwrap();
    h.install_extension(pi_rs_durable::images::create_fit_images_extension(
        Arc::new(Fit),
        Arc::new(|_| Default::default()),
    ))
    .unwrap();
    let id = h
        .create_conversation(Config {
            model: "fake".into(),
            system_prompt: String::new(),
            max_output_tokens: 100,
        })
        .unwrap();
    h.select_extensions(id, vec!["blocks".into(), "fit-images".into()])
        .unwrap();
    h.select_provider_adapter(id, true).unwrap();
    h.set_tool_output_schema(id, "image".into(), Some(json!({"type":"object", "required":["blocks"], "properties":{"blocks":{"type":"array"}}}))).unwrap();
    h.submit_and_run(id, None, "go".into(), &CancellationToken::new())
        .await
        .unwrap();
    let c = h.conversation(id).unwrap();
    let (index, binding) = c
        .content_blocks
        .iter()
        .find(|(i, _)| matches!(c.messages.get(**i), Some(Message::Tool { .. })))
        .unwrap();
    assert_eq!(binding.blocks[1]["data"], "bmV3");
    assert_eq!(
        adapter.0.lock().unwrap()[1]["tool_blocks"][(index + 1).to_string()]["blocks"],
        json!(binding.blocks)
    );
    assert!(
        c.tool_results
            .iter()
            .flat_map(|r| &r.diagnostics)
            .any(|d| d.code.as_deref() == Some("image")
                && d.message == "Converted from image/bmp to image/png.")
    );
    let fork = h.fork(id).unwrap();
    assert_eq!(
        h.conversation(fork).unwrap().content_blocks,
        c.content_blocks
    );
    assert!(!h.request_compaction(id, 0).unwrap());
    h.reset(id, Some("fresh".into())).unwrap();
    assert!(h.conversation(id).unwrap().content_blocks.is_empty());
    drop(h);
    let h = setup(&path);
    assert!(h.conversation(id).unwrap().content_blocks.is_empty());
    assert_eq!(
        h.conversation(fork).unwrap().content_blocks,
        c.content_blocks
    );
    let history = h
        .history(
            id,
            pi_rs_durable::observer::Cursor(0),
            std::num::NonZeroUsize::new(100).unwrap(),
        )
        .unwrap();
    assert!(
        history
            .records
            .iter()
            .any(|r| r.value.get("content_blocks").is_some())
    );
}

struct Tamper;
#[async_trait]
impl Hook for Tamper {
    async fn generation(
        &self,
        event: pi_rs_durable::extension::GenerationEvent,
        value: &Value,
        _: &Context<'_>,
    ) -> Result<pi_rs_durable::extension::GenerationDecision, String> {
        if event == pi_rs_durable::extension::GenerationEvent::BeforeRequest
            && value.get("tool_content_blocks").is_some()
        {
            let mut changed = value.clone();
            changed["tool_content_blocks"] = json!({});
            return Ok(pi_rs_durable::extension::GenerationDecision::Replace(
                changed,
            ));
        }
        Ok(pi_rs_durable::extension::GenerationDecision::Pass)
    }
}
#[tokio::test]
async fn before_request_cannot_discard_tool_blocks() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = setup(&path);
    let adapter = Arc::new(Adapter::default());
    h.install_provider_adapter(adapter.clone()).unwrap();
    h.install_extension(Extension {
        name: "tamper".into(),
        hooks: vec![Arc::new(Tamper)],
        ..Default::default()
    })
    .unwrap();
    let id = h
        .create_conversation(Config {
            model: "fake".into(),
            system_prompt: String::new(),
            max_output_tokens: 100,
        })
        .unwrap();
    h.select_extensions(id, vec!["blocks".into(), "tamper".into()])
        .unwrap();
    h.select_provider_adapter(id, true).unwrap();
    assert!(
        h.submit_and_run(id, None, "go".into(), &CancellationToken::new())
            .await
            .is_err()
    );
    assert_eq!(adapter.0.lock().unwrap().len(), 1);
    drop(h);
    let h = setup(&path);
    let c = h.conversation(id).unwrap();
    assert!(
        c.content_blocks
            .iter()
            .any(|(i, _)| matches!(c.messages.get(*i), Some(Message::Tool { .. })))
    );
}

#[tokio::test]
async fn provider_interruption_child() {
    let Ok(path) = std::env::var("PI_TEST_TOOL_BLOCK_REPLAY_DB") else {
        return;
    };
    let path = std::path::Path::new(&path);
    let mut h = setup(path);
    h.install_provider_adapter(Arc::new(Adapter::default()))
        .unwrap();
    let id = h
        .create_conversation(Config {
            model: "fake".into(),
            system_prompt: String::new(),
            max_output_tokens: 100,
        })
        .unwrap();
    h.select_extensions(id, vec!["blocks".into()]).unwrap();
    h.select_provider_adapter(id, true).unwrap();
    std::fs::write(path.with_extension("id"), id.to_string()).unwrap();
    let _ = h
        .submit_and_run(id, None, "go".into(), &CancellationToken::new())
        .await;
}

#[tokio::test]
async fn killed_provider_attempt_replays_persisted_tool_blocks() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let ready = dir.path().join("ready");
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "provider_interruption_child", "--nocapture"])
        .env("PI_TEST_TOOL_BLOCK_REPLAY_DB", &path)
        .env("PI_TEST_TOOL_BLOCK_PAUSE", &ready)
        .spawn()
        .unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while !ready.exists() {
        assert!(
            child.try_wait().unwrap().is_none(),
            "child exited before provider dispatch"
        );
        if std::time::Instant::now() >= deadline {
            let _ = child.kill();
            panic!("provider did not start");
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    child.kill().unwrap();
    child.wait().unwrap();
    let id = std::fs::read_to_string(path.with_extension("id"))
        .unwrap()
        .parse()
        .unwrap();
    let mut h = setup(&path);
    let adapter = Arc::new(Adapter::default());
    h.install_provider_adapter(adapter.clone()).unwrap();
    let before = h.conversation(id).unwrap().content_blocks;
    h.resume_conversation(id, &CancellationToken::new())
        .await
        .unwrap();
    let after = h.conversation(id).unwrap();
    assert_eq!(after.content_blocks, before);
    assert_eq!(adapter.0.lock().unwrap().len(), 1);
    assert!(
        !adapter.0.lock().unwrap()[0]["tool_blocks"]
            .as_object()
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn invalid_structured_output_is_not_exposed_to_block_hook() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = setup(&path);
    let adapter = Arc::new(Adapter::default());
    h.install_provider_adapter(adapter.clone()).unwrap();
    let id = h
        .create_conversation(Config {
            model: "fake".into(),
            system_prompt: String::new(),
            max_output_tokens: 100,
        })
        .unwrap();
    h.select_extensions(id, vec!["blocks".into()]).unwrap();
    h.select_provider_adapter(id, true).unwrap();
    h.set_tool_output_schema(
        id,
        "image".into(),
        Some(json!({"type":"object", "required":["missing"]})),
    )
    .unwrap();
    assert!(
        h.submit_and_run(id, None, "go".into(), &CancellationToken::new())
            .await
            .is_err()
    );
    assert_eq!(adapter.0.lock().unwrap().len(), 1);
    drop(h);
    let h = setup(&path);
    assert!(h.conversation(id).unwrap().content_blocks.is_empty());
}

#[derive(Default)]
struct SummaryModel(std::sync::Mutex<Vec<ModelRequest>>);
#[async_trait]
impl Model for SummaryModel {
    async fn context_window(&self, _: &str) -> Option<u32> {
        Some(20_000)
    }
    async fn stream(&self, _: ModelRequest) -> Result<ModelStream, Error> {
        panic!("ordinary calls use the adapter")
    }
    async fn stream_for_summary(
        &self,
        _: uuid::Uuid,
        _: uuid::Uuid,
        request: ModelRequest,
    ) -> Result<ModelStream, Error> {
        self.0.lock().unwrap().push(request);
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
fn summary_options(model: Arc<SummaryModel>) -> Options {
    let mut options = options();
    options.model = model;
    options
}
fn summary_setup(path: &std::path::Path, model: Arc<SummaryModel>) -> Harness {
    let mut h = Harness::open_paused(path, summary_options(model)).unwrap();
    h.install_extension(Extension {
        name: "blocks".into(),
        hooks: vec![Arc::new(Blocks)],
        ..Default::default()
    })
    .unwrap();
    h.install_provider_adapter(Arc::new(Adapter::default()))
        .unwrap();
    h
}
#[tokio::test]
async fn manual_compaction_retains_and_then_summarizes_tool_blocks() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let model = Arc::new(SummaryModel::default());
    let mut h = summary_setup(&path, model.clone());
    let id = h
        .create_conversation(Config {
            model: "fake".into(),
            system_prompt: String::new(),
            max_output_tokens: 100,
        })
        .unwrap();
    h.select_extensions(id, vec!["blocks".into()]).unwrap();
    h.select_provider_adapter(id, true).unwrap();
    h.reset(id, Some("older".into())).unwrap();
    h.submit_and_run(id, None, "tool turn".into(), &CancellationToken::new())
        .await
        .unwrap();
    let original = h
        .conversation(id)
        .unwrap()
        .content_blocks
        .values()
        .next()
        .unwrap()
        .clone();
    assert!(h.request_compaction(id, 0).unwrap());
    h.resume_conversation(id, &CancellationToken::new())
        .await
        .unwrap();
    let retained = h.conversation(id).unwrap();
    assert!(retained.content_blocks.values().any(|b| b == &original));
    let fork = h.fork(id).unwrap();
    assert_eq!(
        h.conversation(fork).unwrap().content_blocks,
        retained.content_blocks
    );
    drop(h);
    let mut h = summary_setup(&path, model.clone());
    assert_eq!(
        h.conversation(id).unwrap().content_blocks,
        retained.content_blocks
    );
    h.submit_and_run(id, None, "later turn".into(), &CancellationToken::new())
        .await
        .unwrap();
    assert!(h.request_compaction(id, 0).unwrap());
    h.resume_conversation(id, &CancellationToken::new())
        .await
        .unwrap();
    let c = h.conversation(id).unwrap();
    assert!(c.content_blocks.is_empty());
    let requests = model.0.lock().unwrap();
    let prompt = match &requests.last().unwrap().messages[1] {
        Message::User { content } => content,
        _ => panic!("summary request"),
    };
    assert!(prompt.contains("[Tool result]: before\nafter"));
    assert!(!prompt.contains("aGk="));
    drop(requests);
    let history = h
        .history(
            id,
            pi_rs_durable::observer::Cursor(0),
            std::num::NonZeroUsize::new(1000).unwrap(),
        )
        .unwrap();
    assert!(
        history
            .records
            .iter()
            .any(|r| r.value.to_string().contains("aGk="))
    );
    h.reset(id, Some("new context".into())).unwrap();
    drop(h);
    let h = summary_setup(&path, model);
    assert!(h.conversation(id).unwrap().content_blocks.is_empty());
    assert!(
        h.conversation(fork)
            .unwrap()
            .content_blocks
            .values()
            .any(|b| b == &original)
    );
}

#[tokio::test]
async fn automatic_compaction_summarizes_tool_text_without_image_data() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let model = Arc::new(SummaryModel::default());
    let mut h = summary_setup(&path, model.clone());
    let id = h
        .create_conversation(Config {
            model: "fake".into(),
            system_prompt: String::new(),
            max_output_tokens: 100,
        })
        .unwrap();
    h.select_extensions(id, vec!["blocks".into()]).unwrap();
    h.select_provider_adapter(id, true).unwrap();
    h.submit_and_run(id, None, "x".repeat(90_000), &CancellationToken::new())
        .await
        .unwrap();
    let old = h
        .conversation(id)
        .unwrap()
        .content_blocks
        .values()
        .next()
        .unwrap()
        .message_id;
    h.submit_and_run(id, None, "later".into(), &CancellationToken::new())
        .await
        .unwrap();
    let requests = model.0.lock().unwrap();
    let prompt = match &requests.last().unwrap().messages[1] {
        Message::User { content } => content,
        _ => panic!("summary request"),
    };
    assert!(prompt.contains("[Tool result]: before\nafter"));
    assert!(!prompt.contains("aGk="));
    assert!(
        h.conversation(id)
            .unwrap()
            .content_blocks
            .values()
            .all(|b| b.message_id != old)
    );
}

#[tokio::test]
async fn independent_compaction_retains_and_summarizes_tool_blocks_after_reopen() {
    use pi_rs_durable::{
        extension::Registry,
        runtime::{IndependentCompactionStatus, Scheduler, SummaryProviderIdentity},
    };
    for summarize_tool in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let model = Arc::new(SummaryModel::default());
        let mut h = summary_setup(&path, model.clone());
        let id = h
            .create_conversation(Config {
                model: "fake".into(),
                system_prompt: String::new(),
                max_output_tokens: 100,
            })
            .unwrap();
        h.select_extensions(id, vec!["blocks".into()]).unwrap();
        h.select_provider_adapter(id, true).unwrap();
        h.reset(id, Some("older".into())).unwrap();
        h.submit_and_run(id, None, "tool turn".into(), &CancellationToken::new())
            .await
            .unwrap();
        let original = h
            .conversation(id)
            .unwrap()
            .content_blocks
            .values()
            .next()
            .unwrap()
            .clone();
        if summarize_tool {
            h.submit_and_run(id, None, "later".into(), &CancellationToken::new())
                .await
                .unwrap();
        }
        drop(h);
        let mut registry = Registry::default();
        registry
            .install(Extension {
                name: "blocks".into(),
                hooks: vec![Arc::new(Blocks)],
                ..Default::default()
            })
            .unwrap();
        let identity = SummaryProviderIdentity {
            model: "summary-fixture/v1".into(),
            settlement: "ledger-fixture/v1".into(),
        };
        let mut scheduler = Scheduler::open_with_summary_recovery(
            &path,
            summary_options(model.clone()),
            registry,
            Some(Arc::new(Adapter::default())),
            identity,
        )
        .unwrap();
        let op = uuid::Uuid::new_v4();
        assert!(
            scheduler
                .request_idle_independent_compaction(id, op, 0)
                .unwrap()
        );
        assert_eq!(
            scheduler.wait_independent_compaction(op).await.unwrap(),
            IndependentCompactionStatus::Applied
        );
        let c = scheduler.conversation(id).unwrap();
        if summarize_tool {
            assert!(c.content_blocks.is_empty());
            let requests = model.0.lock().unwrap();
            let prompt = match &requests.last().unwrap().messages[1] {
                Message::User { content } => content,
                _ => panic!("summary request"),
            };
            assert!(prompt.contains("[Tool result]: before\nafter"));
            assert!(!prompt.contains("aGk="));
        } else {
            assert!(c.content_blocks.values().any(|b| b == &original));
            let fork = scheduler.fork(id).unwrap();
            assert_eq!(
                scheduler.conversation(fork).unwrap().content_blocks,
                c.content_blocks
            );
        }
        scheduler.shutdown().await.unwrap();
        let h = summary_setup(&path, model);
        assert_eq!(h.conversation(id).unwrap().content_blocks, c.content_blocks);
    }
}

struct GateSummary {
    started: tokio::sync::Semaphore,
    release: tokio::sync::Semaphore,
}
#[async_trait]
impl Model for GateSummary {
    async fn stream(&self, _: ModelRequest) -> Result<ModelStream, Error> {
        panic!("ordinary calls use adapter")
    }
    async fn stream_for_summary(
        &self,
        _: uuid::Uuid,
        _: uuid::Uuid,
        _: ModelRequest,
    ) -> Result<ModelStream, Error> {
        self.started.add_permits(1);
        self.release.acquire().await.unwrap().forget();
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
async fn independent_tool_summary_recovery_and_reset_are_fenced() {
    use pi_rs_durable::{
        extension::Registry,
        runtime::{IndependentCompactionStatus, Scheduler, SummaryProviderIdentity},
    };
    for reset in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("db");
        let seed_model = Arc::new(SummaryModel::default());
        let mut h = summary_setup(&path, seed_model);
        let id = h
            .create_conversation(Config {
                model: "fake".into(),
                system_prompt: String::new(),
                max_output_tokens: 100,
            })
            .unwrap();
        h.select_extensions(id, vec!["blocks".into()]).unwrap();
        h.select_provider_adapter(id, true).unwrap();
        h.reset(id, Some("older".into())).unwrap();
        h.submit_and_run(id, None, "tool turn".into(), &CancellationToken::new())
            .await
            .unwrap();
        let original = h.conversation(id).unwrap().content_blocks;
        drop(h);
        let model = Arc::new(GateSummary {
            started: tokio::sync::Semaphore::new(0),
            release: tokio::sync::Semaphore::new(0),
        });
        let open = || {
            let mut registry = Registry::default();
            registry
                .install(Extension {
                    name: "blocks".into(),
                    hooks: vec![Arc::new(Blocks)],
                    ..Default::default()
                })
                .unwrap();
            let mut opts = options();
            opts.model = model.clone();
            Scheduler::open_with_summary_recovery(
                &path,
                opts,
                registry,
                Some(Arc::new(Adapter::default())),
                SummaryProviderIdentity {
                    model: "summary-fixture/v1".into(),
                    settlement: "ledger-fixture/v1".into(),
                },
            )
            .unwrap()
        };
        let mut scheduler = open();
        let op = uuid::Uuid::new_v4();
        assert!(
            scheduler
                .request_idle_independent_compaction(id, op, 0)
                .unwrap()
        );
        tokio::time::timeout(std::time::Duration::from_secs(5), model.started.acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();
        if reset {
            scheduler.reset(id, Some("reset".into())).unwrap();
        } else {
            scheduler.shutdown().await.unwrap();
            scheduler = open();
            tokio::time::timeout(std::time::Duration::from_secs(5), model.started.acquire())
                .await
                .unwrap()
                .unwrap()
                .forget();
        }
        model.release.add_permits(1);
        let expected = if reset {
            IndependentCompactionStatus::Stale
        } else {
            IndependentCompactionStatus::Applied
        };
        assert_eq!(
            scheduler.wait_independent_compaction(op).await.unwrap(),
            expected
        );
        let c = scheduler.conversation(id).unwrap();
        if reset {
            assert!(c.content_blocks.is_empty());
        } else {
            assert_eq!(c.content_blocks.values().next(), original.values().next());
        }
        scheduler.shutdown().await.unwrap();
    }
}

struct BusyAdapter {
    started: tokio::sync::Semaphore,
    release: Arc<tokio::sync::Semaphore>,
}
#[async_trait]
impl pi_rs_durable::model::ProviderAdapter for BusyAdapter {
    fn id(&self) -> &str {
        "tool-block-fixture/v1"
    }
    fn supports_durable_tool_blocks(&self) -> bool {
        true
    }
    async fn prepare(
        &self,
        _: &pi_rs_durable::model::ProviderAttempt,
        _: &ModelRequest,
    ) -> Result<Value, Error> {
        Ok(json!({}))
    }
    fn validate(
        &self,
        _: &pi_rs_durable::model::ProviderAttempt,
        _: &ModelRequest,
        _: &Value,
    ) -> Result<(), Error> {
        Ok(())
    }
    async fn prepare_with_tool_blocks(
        &self,
        _: &pi_rs_durable::model::ProviderAttempt,
        request: &pi_rs_durable::model::DurableToolRequest,
    ) -> Result<Value, Error> {
        request.validate_bindings()?;
        Ok(json!({"tool_blocks":request.tool_blocks}))
    }
    fn validate_with_tool_blocks(
        &self,
        _: &pi_rs_durable::model::ProviderAttempt,
        request: &pi_rs_durable::model::DurableToolRequest,
        body: &Value,
    ) -> Result<(), Error> {
        request.validate_bindings()?;
        if body["tool_blocks"] != json!(request.tool_blocks) {
            return Err(Error::Config("changed".into()));
        }
        Ok(())
    }
    async fn dispatch(
        &self,
        _: &pi_rs_durable::model::ProviderAttempt,
        _: &ModelRequest,
        _: Value,
    ) -> Result<ModelStream, Error> {
        self.started.add_permits(1);
        let release = self.release.clone();
        Ok(stream::once(async move {
            release.acquire().await.unwrap().forget();
            Ok(ModelEvent::Text("busy answer".into()))
        })
        .chain(stream::iter(vec![Ok(ModelEvent::Complete {
            calls: vec![],
            usage: None,
        })]))
        .boxed())
    }
}
#[tokio::test]
async fn busy_independent_compaction_preserves_retained_tool_blocks() {
    use pi_rs_durable::{
        extension::Registry,
        runtime::{IndependentCompactionStatus, Scheduler, SummaryProviderIdentity},
    };
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let model = Arc::new(SummaryModel::default());
    let mut h = summary_setup(&path, model.clone());
    let id = h
        .create_conversation(Config {
            model: "fake".into(),
            system_prompt: String::new(),
            max_output_tokens: 100,
        })
        .unwrap();
    h.select_extensions(id, vec!["blocks".into()]).unwrap();
    h.select_provider_adapter(id, true).unwrap();
    h.reset(id, Some("older".repeat(3000))).unwrap();
    h.submit_and_run(id, None, "tool turn".into(), &CancellationToken::new())
        .await
        .unwrap();
    let original = h
        .conversation(id)
        .unwrap()
        .content_blocks
        .values()
        .next()
        .unwrap()
        .clone();
    drop(h);
    let mut registry = Registry::default();
    registry
        .install(Extension {
            name: "blocks".into(),
            hooks: vec![Arc::new(Blocks)],
            ..Default::default()
        })
        .unwrap();
    let adapter = Arc::new(BusyAdapter {
        started: tokio::sync::Semaphore::new(0),
        release: Arc::new(tokio::sync::Semaphore::new(0)),
    });
    let mut scheduler = Scheduler::open_with_summary_recovery(
        &path,
        summary_options(model),
        registry,
        Some(adapter.clone()),
        SummaryProviderIdentity {
            model: "summary-fixture/v1".into(),
            settlement: "ledger-fixture/v1".into(),
        },
    )
    .unwrap();
    scheduler.submit(id, None, "latest".into()).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), adapter.started.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    let op = uuid::Uuid::new_v4();
    assert!(
        scheduler
            .request_busy_independent_compaction(id, op, 1500)
            .unwrap()
    );
    assert_eq!(
        scheduler.wait_independent_compaction(op).await.unwrap(),
        IndependentCompactionStatus::Running
    );
    adapter.release.add_permits(1);
    scheduler.wait(id).await.unwrap();
    assert_eq!(
        scheduler.independent_compaction_status(op).unwrap(),
        IndependentCompactionStatus::Applied
    );
    assert!(
        scheduler
            .conversation(id)
            .unwrap()
            .content_blocks
            .values()
            .any(|b| b == &original)
    );
    scheduler.shutdown().await.unwrap();
}

struct OverflowAdapter(std::sync::atomic::AtomicBool);
#[async_trait]
impl pi_rs_durable::model::ProviderAdapter for OverflowAdapter {
    fn id(&self) -> &str {
        "tool-block-fixture/v1"
    }
    fn supports_durable_tool_blocks(&self) -> bool {
        true
    }
    async fn prepare(
        &self,
        _: &pi_rs_durable::model::ProviderAttempt,
        _: &ModelRequest,
    ) -> Result<Value, Error> {
        Ok(json!({}))
    }
    fn validate(
        &self,
        _: &pi_rs_durable::model::ProviderAttempt,
        _: &ModelRequest,
        _: &Value,
    ) -> Result<(), Error> {
        Ok(())
    }
    async fn prepare_with_tool_blocks(
        &self,
        _: &pi_rs_durable::model::ProviderAttempt,
        request: &pi_rs_durable::model::DurableToolRequest,
    ) -> Result<Value, Error> {
        request.validate_bindings()?;
        Ok(json!({"tool_blocks":request.tool_blocks}))
    }
    fn validate_with_tool_blocks(
        &self,
        _: &pi_rs_durable::model::ProviderAttempt,
        request: &pi_rs_durable::model::DurableToolRequest,
        body: &Value,
    ) -> Result<(), Error> {
        request.validate_bindings()?;
        if body["tool_blocks"] != json!(request.tool_blocks) {
            return Err(Error::Config("changed".into()));
        }
        Ok(())
    }
    async fn dispatch(
        &self,
        _: &pi_rs_durable::model::ProviderAttempt,
        _: &ModelRequest,
        _: Value,
    ) -> Result<ModelStream, Error> {
        if self.0.swap(false, std::sync::atomic::Ordering::SeqCst) {
            return Err(Error::Model("context length exceeded".into()));
        }
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
#[tokio::test]
async fn provider_overflow_compacts_tool_block_text_before_retry() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let model = Arc::new(SummaryModel::default());
    let mut h = summary_setup(&path, model.clone());
    let id = h
        .create_conversation(Config {
            model: "fake".into(),
            system_prompt: String::new(),
            max_output_tokens: 100,
        })
        .unwrap();
    h.select_extensions(id, vec!["blocks".into()]).unwrap();
    h.select_provider_adapter(id, true).unwrap();
    h.submit_and_run(id, None, "tool turn".into(), &CancellationToken::new())
        .await
        .unwrap();
    drop(h);
    let mut h = Harness::open_paused(&path, summary_options(model.clone())).unwrap();
    h.install_extension(Extension {
        name: "blocks".into(),
        hooks: vec![Arc::new(Blocks)],
        ..Default::default()
    })
    .unwrap();
    h.install_provider_adapter(Arc::new(OverflowAdapter(true.into())))
        .unwrap();
    h.submit_and_run(id, None, "later".into(), &CancellationToken::new())
        .await
        .unwrap();
    let requests = model.0.lock().unwrap();
    let prompt = match &requests.last().unwrap().messages[1] {
        Message::User { content } => content,
        _ => panic!("summary request"),
    };
    assert!(prompt.contains("[Tool result]: before\nafter"));
    assert!(!prompt.contains("aGk="));
    assert!(h.conversation(id).unwrap().content_blocks.is_empty());
}

#[tokio::test]
async fn tool_block_compaction_rejects_hooks_without_block_opt_in() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let model = Arc::new(SummaryModel::default());
    let mut h = summary_setup(&path, model.clone());
    let id = h
        .create_conversation(Config {
            model: "fake".into(),
            system_prompt: String::new(),
            max_output_tokens: 100,
        })
        .unwrap();
    h.select_extensions(id, vec!["blocks".into()]).unwrap();
    h.select_provider_adapter(id, true).unwrap();
    h.reset(id, Some("older".into())).unwrap();
    h.submit_and_run(id, None, "tool turn".into(), &CancellationToken::new())
        .await
        .unwrap();
    let before = h.conversation(id).unwrap();
    struct Unaware;
    #[async_trait]
    impl Hook for Unaware {
        fn supports_compaction_blocks(&self) -> bool {
            true
        }
    }
    h.install_extension(Extension {
        name: "blocks".into(),
        hooks: vec![Arc::new(Unaware)],
        ..Default::default()
    })
    .unwrap();
    assert!(h.request_compaction(id, 0).is_err());
    let after = h.conversation(id).unwrap();
    assert_eq!(after.messages, before.messages);
    assert_eq!(after.content_blocks, before.content_blocks);
    assert!(model.0.lock().unwrap().is_empty());
}

#[cfg(feature = "builtin-images")]
#[tokio::test]
async fn builtin_fitter_uses_conversation_model_limits_for_tool_output() {
    use base64::Engine;
    use image::{DynamicImage, ImageFormat, RgbaImage};
    use pi_rs_durable::images::{BuiltinImageProcessor, ImageLimitsCatalog};
    struct Picture(String);
    #[async_trait]
    impl Hook for Picture {
        fn supports_runtime_tool_blocks(&self) -> bool {
            true
        }
        async fn after_tool_blocks(
            &self,
            _: &HookCall,
            value: &ToolBlockResult,
            _: bool,
            _: &Context<'_>,
        ) -> Result<Option<ToolBlockResult>, String> {
            let mut result = value.clone();
            result.output = vec![json!({"type":"image","data":self.0,"mimeType":"image/png"})];
            Ok(Some(result))
        }
    }
    let image =
        DynamicImage::ImageRgba8(RgbaImage::from_pixel(8, 6, image::Rgba([20, 40, 60, 255])));
    let mut encoded = std::io::Cursor::new(Vec::new());
    image.write_to(&mut encoded, ImageFormat::Png).unwrap();
    let image = base64::engine::general_purpose::STANDARD.encode(encoded.into_inner());
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let mut h = Harness::open_paused(&path, options()).unwrap();
    h.install_extension(Extension {
        name: "picture".into(),
        hooks: vec![Arc::new(Picture(image))],
        ..Default::default()
    })
    .unwrap();
    let mut catalog = ImageLimitsCatalog::default();
    catalog
        .insert(
            "fake",
            &json!({"inputLimits":{"images":{"resize":{"maxWidth":4}}}}),
        )
        .unwrap();
    h.install_extension(pi_rs_durable::images::create_fit_images_extension(
        Arc::new(BuiltinImageProcessor),
        catalog.resolver(),
    ))
    .unwrap();
    let adapter = Arc::new(Adapter::default());
    h.install_provider_adapter(adapter.clone()).unwrap();
    let id = h
        .create_conversation(Config {
            model: "fake".into(),
            system_prompt: String::new(),
            max_output_tokens: 100,
        })
        .unwrap();
    h.select_extensions(id, vec!["picture".into(), "fit-images".into()])
        .unwrap();
    h.select_provider_adapter(id, true).unwrap();
    h.submit_and_run(id, None, "show".into(), &CancellationToken::new())
        .await
        .unwrap();
    let c = h.conversation(id).unwrap();
    let binding = c.content_blocks.values().next().unwrap();
    let data = base64::engine::general_purpose::STANDARD
        .decode(binding.blocks[0]["data"].as_str().unwrap())
        .unwrap();
    let result = image::load_from_memory(&data).unwrap();
    assert_eq!((result.width(), result.height()), (4, 3));
    assert!(
        c.tool_results
            .iter()
            .flat_map(|r| &r.diagnostics)
            .any(|d| d.code.as_deref() == Some("image")
                && d.message.contains("Resized from 8x6 to 4x3"))
    );
    drop(h);
    let h = Harness::open_paused(&path, options()).unwrap();
    assert_eq!(h.conversation(id).unwrap().content_blocks, c.content_blocks);
    assert_eq!(adapter.0.lock().unwrap().len(), 2);
}
