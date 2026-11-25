// Included by each bundled provider's HTTP tests. The host uses an explicit
// text-only override before and after reopening, while the stored result retains
// its original image and error metadata.
use pi_rs_durable::{
    extension::{
        Context, Extension, GenerationDecision, GenerationEvent, Hook, ToolBlockResult,
        ToolCall as HookCall,
    },
    recovery::{ReplayPolicy, UncertainPolicy},
    runtime::{Config, Harness, Options, RegisteredTool},
};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

struct Unused;
#[async_trait::async_trait]
impl pi_rs_core::Model for Unused {
    async fn stream(&self, _: ModelRequest) -> pi_rs_core::Result<pi_rs_core::ModelStream> {
        panic!("provider required")
    }
}
struct Failing(Arc<AtomicUsize>);
#[async_trait::async_trait]
impl pi_rs_core::Tool for Failing {
    fn spec(&self) -> pi_rs_core::ToolSpec {
        pi_rs_core::ToolSpec {
            name: "read".into(),
            description: String::new(),
            parameters: json!({"type":"object"}),
        }
    }
    async fn invoke(&self, _: Value, _: pi_rs_core::ToolContext) -> pi_rs_core::Outcome {
        self.0.fetch_add(1, Ordering::SeqCst);
        pi_rs_core::Outcome::Failed("failure".into())
    }
}
struct Blocks(AtomicBool);
#[async_trait::async_trait]
impl Hook for Blocks {
    fn supports_runtime_tool_blocks(&self) -> bool {
        true
    }
    async fn after_tool_blocks(
        &self,
        _: &HookCall,
        result: &ToolBlockResult,
        _: bool,
        _: &Context<'_>,
    ) -> Result<Option<ToolBlockResult>, String> {
        let mut result = result.clone();
        result.output = vec![
            json!({"type":"text","text":"before"}),
            json!({"type":"image","mimeType":"image/png","data":"aGk="}),
            json!({"type":"text","text":"after"}),
        ];
        Ok(Some(result))
    }
    async fn generation(
        &self,
        event: GenerationEvent,
        value: &Value,
        _: &Context<'_>,
    ) -> Result<GenerationDecision, String> {
        if event == GenerationEvent::BeforeRequest
            && value.get("tool_content_blocks").is_some()
            && self.0.load(Ordering::SeqCst)
        {
            let mut changed = value.clone();
            changed["tool_content_blocks"] = json!({});
            return Ok(GenerationDecision::Replace(changed));
        }
        Ok(GenerationDecision::Pass)
    }
}
#[tokio::test]
async fn status_and_profile_survive_reopen_and_fork() {
    let (url, rx, thread) = server(http(LIFECYCLE_CALL), false);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let calls = Arc::new(AtomicUsize::new(0));
    let options = || Options {
        model: Arc::new(Unused),
        tools: vec![RegisteredTool {
            tool: Arc::new(Failing(calls.clone())),
            replay: ReplayPolicy::Unsafe,
        }],
        restart_limit: 3,
        max_turns: 3,
        uncertain_policy: UncertainPolicy::InterruptedResult,
    };
    let hook = Arc::new(Blocks(AtomicBool::new(true)));
    let extension = || Extension {
        name: "blocks".into(),
        hooks: vec![hook.clone()],
        ..Default::default()
    };
    let mut h = Harness::open_paused(&path, options()).unwrap();
    h.install_provider_adapter(lifecycle_adapter(&url)).unwrap();
    h.install_extension(extension()).unwrap();
    let id = h
        .create_conversation(Config {
            model: "test".into(),
            system_prompt: String::new(),
            max_output_tokens: 100,
        })
        .unwrap();
    h.select_provider_adapter(id, true).unwrap();
    h.select_extensions(id, vec!["blocks".into()]).unwrap();
    let profile = h.model_image_profile(id).unwrap().unwrap();
    assert!(!profile.supports_images);
    assert_eq!(profile.limits.max_width, 512);
    assert!(
        h.submit_and_run(id, None, "go".into(), &CancellationToken::new())
            .await
            .is_err()
    );
    rx.recv().unwrap();
    thread.join().unwrap();
    drop(h);
    hook.0.store(false, Ordering::SeqCst);
    let (url, rx, thread) = server(http(LIFECYCLE_DONE), false);
    let mut h = Harness::open_paused(&path, options()).unwrap();
    h.install_provider_adapter(lifecycle_adapter(&url)).unwrap();
    h.install_extension(extension()).unwrap();
    let before = h.conversation(id).unwrap();
    let occurrence = before
        .content_blocks
        .values()
        .find(|b| b.blocks.len() == 3)
        .unwrap()
        .message_id;
    assert_eq!(
        before
            .tool_results
            .iter()
            .find(|m| m.call_id == "c1")
            .unwrap()
            .content_block_id,
        Some(occurrence)
    );
    h.resume_conversation(id, &CancellationToken::new())
        .await
        .unwrap();
    let body = rx.recv().unwrap();
    lifecycle_assert(&body);
    assert!(!body.to_string().contains("aGk="));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let fork = h.fork(id).unwrap();
    let state = h.conversation(fork).unwrap();
    assert_eq!(state.content_blocks, before.content_blocks);
    assert!(
        state
            .tool_results
            .iter()
            .any(|m| m.content_block_id == Some(occurrence) && m.is_error)
    );
    drop(h);
    thread.join().unwrap();
    let h = Harness::open_paused(&path, options()).unwrap();
    let state = h.conversation(fork).unwrap();
    assert_eq!(state.content_blocks, before.content_blocks);
    assert!(
        state
            .tool_results
            .iter()
            .any(|m| m.content_block_id == Some(occurrence) && m.is_error)
    );
}

#[test]
fn legacy_tool_metadata_does_not_invent_occurrence_identity() {
    let old: pi_rs_durable::runtime::ToolResultMetadata = serde_json::from_value(json!({
        "operation_id":Uuid::new_v4(),"call_id":"c1","tool":"read","is_error":true
    }))
    .unwrap();
    assert!(old.is_error);
    assert_eq!(old.content_block_id, None);
}
