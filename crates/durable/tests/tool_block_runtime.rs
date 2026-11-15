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
    assert!(h.request_compaction(id, 0).is_err());
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
