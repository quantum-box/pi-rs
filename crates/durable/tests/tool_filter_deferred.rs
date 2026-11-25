use futures_util::{StreamExt, stream};
use pi_rs_core::{
    CancellationToken, Message, Model, ModelEvent, ModelRequest, ModelStream, Outcome, Tool,
    ToolCall, ToolContext, ToolSpec,
};
use pi_rs_durable::{
    extension::{Context, Extension, GenerationDecision, GenerationEvent, Hook},
    keyed_tool::{KeyedHost, KeyedTool, KeyedToolAdapter},
    recovery::{ReplayPolicy, UncertainPolicy},
    reset::ToolControl,
    runtime::{Config, Harness, Options, RegisteredTool},
};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

#[derive(Default)]
struct RecordingModel {
    requests: Mutex<Vec<Vec<String>>>,
    calls: AtomicUsize,
}
#[async_trait::async_trait]
impl Model for RecordingModel {
    async fn stream(&self, request: ModelRequest) -> pi_rs_core::Result<ModelStream> {
        self.requests
            .lock()
            .unwrap()
            .push(request.tools.iter().map(|t| t.name.clone()).collect());
        let calls = if self.calls.fetch_add(1, Ordering::SeqCst) == 0 {
            vec![ToolCall {
                id: "load".into(),
                name: "loader".into(),
                arguments: "{}".into(),
            }]
        } else {
            vec![]
        };
        Ok(stream::iter(vec![
            Ok(ModelEvent::Text("answer".into())),
            Ok(ModelEvent::Complete { calls, usage: None }),
        ])
        .boxed())
    }
}
struct PlainTool(&'static str);
#[async_trait::async_trait]
impl pi_rs_core::Tool for PlainTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: self.0.into(),
            description: String::new(),
            parameters: serde_json::json!({"type":"object"}),
        }
    }
    async fn invoke(&self, _: serde_json::Value, _: ToolContext) -> Outcome {
        Outcome::Succeeded("ok".into())
    }
}
struct Loader(Arc<AtomicUsize>);
#[async_trait::async_trait]
impl KeyedTool for Loader {
    fn spec(&self) -> ToolSpec {
        PlainTool("loader").spec()
    }
    async fn invoke(
        &self,
        _: serde_json::Value,
        _: ToolContext,
        _: Option<&pi_rs_core::ExtensionState>,
        _: KeyedHost,
    ) -> pi_rs_core::ToolReply {
        unreachable!()
    }
    async fn invoke_controlled(
        &self,
        _: serde_json::Value,
        _: ToolContext,
        _: Option<&pi_rs_core::ExtensionState>,
        host: KeyedHost,
    ) -> (pi_rs_core::ToolReply, ToolControl) {
        self.0.fetch_add(1, Ordering::SeqCst);
        assert!(
            matches!(host.call_keyed("nested", "deferred", serde_json::json!({})).await, Outcome::Succeeded(value) if value == "ok")
        );
        (
            Outcome::Succeeded("loaded".into()).into(),
            ToolControl {
                add_tools: vec![
                    "deferred".into(),
                    "restore".into(),
                    "missing".into(),
                    "deferred".into(),
                ],
                ..Default::default()
            },
        )
    }
}
struct ObserveHook(Arc<Mutex<Vec<serde_json::Value>>>);
#[async_trait::async_trait]
impl Hook for ObserveHook {
    async fn generation(
        &self,
        event: GenerationEvent,
        value: &serde_json::Value,
        context: &Context<'_>,
    ) -> Result<GenerationDecision, String> {
        if event == GenerationEvent::BeforeRequest {
            self.0
                .lock()
                .unwrap()
                .push(context.snapshot["tool_filters"].clone());
            return Ok(GenerationDecision::Replace(
                serde_json::json!({"messages": value["messages"]}),
            ));
        }
        Ok(GenerationDecision::Pass)
    }
}
fn options(model: Arc<RecordingModel>, effects: Arc<AtomicUsize>) -> Options {
    let mut tools: Vec<RegisteredTool> = ["normal", "deferred", "restore"]
        .into_iter()
        .map(|name| RegisteredTool {
            tool: Arc::new(PlainTool(name)),
            replay: ReplayPolicy::Safe,
        })
        .collect();
    tools.push(Arc::new(KeyedToolAdapter::new(Loader(effects))).register(ReplayPolicy::Safe));
    Options {
        model,
        tools,
        restart_limit: 3,
        max_turns: 4,
        uncertain_policy: UncertainPolicy::RequireReconciliation,
    }
}
fn hook(h: &Harness, seen: Arc<Mutex<Vec<serde_json::Value>>>) {
    h.install_extension(Extension {
        name: "before".into(),
        hooks: vec![Arc::new(ObserveHook(seen))],
        ..Default::default()
    })
    .unwrap();
}
fn config() -> Config {
    Config {
        model: "test".into(),
        system_prompt: String::new(),
        max_output_tokens: 64,
    }
}

#[tokio::test]
async fn control_loads_deferred_and_clears_removals_before_next_request_and_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let model = Arc::new(RecordingModel::default());
    let effects = Arc::new(AtomicUsize::new(0));
    let seen = Arc::new(Mutex::new(vec![]));
    let id;
    {
        let mut h = Harness::open_paused(&path, options(model.clone(), effects.clone())).unwrap();
        hook(&h, seen.clone());
        id = h.create_conversation(config()).unwrap();
        h.select_extensions(id, vec!["before".into()]).unwrap();
        h.select_tools_except(id, vec!["restore".into()]).unwrap();
        h.set_deferred_tools(
            id,
            vec!["deferred".into(), "restore".into(), "missing".into()],
        )
        .unwrap();
        h.select_model_tools_patch(id, vec!["deferred".into(), "restore".into()], vec![])
            .unwrap();
        h.submit_and_run(id, None, "go".into(), &CancellationToken::new())
            .await
            .unwrap();
        assert_eq!(
            *model.requests.lock().unwrap(),
            vec![
                vec!["loader", "normal"],
                vec!["loader", "normal", "deferred", "restore"]
            ]
        );
        assert_eq!(effects.load(Ordering::SeqCst), 1);
        let state = h.conversation(id).unwrap();
        assert!(state.tool_filters.excluded.is_empty());
        assert!(state.tool_filters.model_excluded.is_empty());
        assert_eq!(
            state.tool_filters.model_add,
            vec!["deferred", "restore", "missing"]
        );
        assert_eq!(seen.lock().unwrap().len(), 2);
        assert_eq!(seen.lock().unwrap()[0]["model_add"], serde_json::json!([]));
        assert_eq!(
            seen.lock().unwrap()[1]["model_add"],
            serde_json::json!(["deferred", "restore", "missing"])
        );
    }
    let mut reopened = options(model.clone(), effects.clone());
    reopened.tools.push(RegisteredTool {
        tool: Arc::new(PlainTool("missing")),
        replay: ReplayPolicy::Safe,
    });
    let mut h = Harness::open_paused(&path, reopened).unwrap();
    hook(&h, seen.clone());
    let fork = h.fork(id).unwrap();
    assert_eq!(
        h.conversation(fork).unwrap().tool_filters,
        h.conversation(id).unwrap().tool_filters
    );
    h.submit_and_run(id, None, "again".into(), &CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(
        model.requests.lock().unwrap().last().unwrap(),
        &vec!["loader", "normal", "deferred", "restore", "missing"]
    );
    assert_eq!(effects.load(Ordering::SeqCst), 1);
}

struct CancelAfterTool(CancellationToken);
#[async_trait::async_trait]
impl Hook for CancelAfterTool {
    async fn after_tool(
        &self,
        call: &pi_rs_durable::extension::ToolCall,
        _: &serde_json::Value,
        _: &Context<'_>,
    ) -> Result<Option<serde_json::Value>, String> {
        if call.name == "loader" {
            self.0.cancel();
        }
        Ok(None)
    }
}

#[tokio::test]
async fn committed_load_control_survives_interruption_before_round_boundary() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let model = Arc::new(RecordingModel::default());
    let effects = Arc::new(AtomicUsize::new(0));
    let cancel = CancellationToken::new();
    let id;
    {
        let mut h = Harness::open_paused(&path, options(model.clone(), effects.clone())).unwrap();
        h.install_extension(Extension {
            name: "cancel".into(),
            hooks: vec![Arc::new(CancelAfterTool(cancel.clone()))],
            ..Default::default()
        })
        .unwrap();
        id = h.create_conversation(config()).unwrap();
        h.select_extensions(id, vec!["cancel".into()]).unwrap();
        h.select_tools_except(id, vec!["restore".into()]).unwrap();
        h.set_deferred_tools(id, vec!["deferred".into(), "restore".into()])
            .unwrap();
        h.select_model_tools_patch(id, vec!["deferred".into(), "restore".into()], vec![])
            .unwrap();
        assert!(
            h.submit_and_run(id, None, "go".into(), &cancel)
                .await
                .is_err()
        );
        assert_eq!(effects.load(Ordering::SeqCst), 1);
    }
    let mut h = Harness::open_paused(&path, options(model.clone(), effects.clone())).unwrap();
    h.install_extension(Extension {
        name: "cancel".into(),
        ..Default::default()
    })
    .unwrap();
    h.resume_conversation(id, &CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    assert_eq!(
        model.requests.lock().unwrap().last().unwrap(),
        &vec!["loader", "normal", "deferred", "restore"]
    );
    assert_eq!(
        h.conversation(id).unwrap().tool_filters.model_add,
        vec!["deferred", "restore", "missing"]
    );
}

#[tokio::test]
async fn exact_selection_overrides_deferred_and_exclusion_can_be_replaced() {
    let dir = tempfile::tempdir().unwrap();
    let model = Arc::new(RecordingModel::default());
    let effects = Arc::new(AtomicUsize::new(0));
    let mut h = Harness::open_paused(
        dir.path().join("db"),
        options(model.clone(), effects.clone()),
    )
    .unwrap();
    let id = h.create_conversation(config()).unwrap();
    h.set_deferred_tools(id, vec!["deferred".into(), "restore".into()])
        .unwrap();
    h.select_tools_except(id, vec!["restore".into()]).unwrap();
    h.select_model_tools(
        id,
        Some(vec!["restore".into(), "deferred".into(), "normal".into()]),
    )
    .unwrap();
    h.submit_and_run(id, None, "test".into(), &CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(
        model.requests.lock().unwrap()[0],
        vec!["deferred", "normal"]
    );
    assert_eq!(effects.load(Ordering::SeqCst), 0);
    assert!(
        h.conversation(id)
            .unwrap()
            .messages
            .iter()
            .any(|m| matches!(m, Message::Tool { content, .. } if content.contains("unavailable")))
    );
    h.select_tools(id, Some(vec!["restore".into(), "deferred".into()]))
        .unwrap();
    h.select_model_tools_patch(id, vec!["restore".into()], vec!["deferred".into()])
        .unwrap();
    h.submit_and_run(id, None, "again".into(), &CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(
        model.requests.lock().unwrap().last().unwrap(),
        &vec!["deferred"]
    );
    h.select_tools(id, None).unwrap();
    h.select_model_tools_patch(id, vec!["normal".into()], vec!["restore".into()])
        .unwrap();
    h.submit_and_run(id, None, "third".into(), &CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(
        model.requests.lock().unwrap().last().unwrap(),
        &vec!["loader", "restore"]
    );
}
