use pi_rs_core::{
    Error, Extension, Model, ModelEvent, ModelRequest, ModelStream, Outcome, Registry,
    RegistryRefresher, RegistryUpdate, Result, SessionBuilder, SessionEvent, Tool, ToolCall,
    ToolContext, ToolInventoryPhase, ToolSpec,
};
use serde_json::{Value, json};
use std::{
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

struct VersionedExtension(&'static str);
impl Extension for VersionedExtension {
    fn id(&self) -> &str {
        "versioned"
    }
    fn command_source(&self) -> Option<&Path> {
        Some(Path::new(self.0))
    }
    fn tools(&self) -> Vec<Arc<dyn Tool>> {
        vec![
            Arc::new(VersionedTool(self.0, "fixture")),
            Arc::new(VersionedTool(self.0, "hidden")),
        ]
    }
}
struct VersionedTool(&'static str, &'static str);
#[async_trait::async_trait]
impl Tool for VersionedTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: self.1.into(),
            description: self.0.into(),
            parameters: json!({"type":"object"}),
        }
    }
    async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
        Outcome::Succeeded(self.0.into())
    }
}
struct Refresh(AtomicUsize);
#[async_trait::async_trait]
impl RegistryRefresher for Refresh {
    async fn refresh(&self, current: &Registry) -> Result<Option<RegistryUpdate>> {
        let version = match self.0.fetch_add(1, Ordering::SeqCst) {
            0 => "/request",
            1 => "/dispatch",
            _ => return Ok(None),
        };
        let mut registry = current.clone();
        assert!(registry.replace(Arc::new(VersionedExtension(version)))?);
        Ok(Some(RegistryUpdate {
            registry,
            system_prompt: None,
        }))
    }
}
struct Script(Mutex<Vec<ModelRequest>>);
#[async_trait::async_trait]
impl Model for Script {
    async fn stream(&self, request: ModelRequest) -> Result<ModelStream> {
        let mut requests = self.0.lock().unwrap();
        requests.push(request);
        let events = if requests.len() == 1 {
            vec![ModelEvent::Complete {
                calls: vec![ToolCall {
                    id: "call".into(),
                    name: "fixture".into(),
                    arguments: "{}".into(),
                }],
                usage: None,
            }]
        } else {
            vec![
                ModelEvent::Text("done".into()),
                ModelEvent::Complete {
                    calls: vec![],
                    usage: None,
                },
            ]
        };
        Ok(Box::pin(futures_util::stream::iter(
            events.into_iter().map(Ok),
        )))
    }
}
#[tokio::test]
async fn runtime_events_report_actual_request_and_dispatch_registries() {
    let refresh = Arc::new(Refresh(AtomicUsize::new(0)));
    let mut registry = Registry::default();
    registry
        .register(Arc::new(VersionedExtension("/configured")))
        .unwrap();
    registry.set_active_tools(["fixture".into()]).unwrap();
    registry.set_refresher(refresh.clone());
    let model = Arc::new(Script(Mutex::new(vec![])));
    let mut session = SessionBuilder::new(model.clone(), "fixture")
        .registry(registry)
        .in_memory()
        .unwrap();
    let mut events = session.subscribe();
    assert_eq!(session.prompt("test").await.unwrap(), "done");
    assert_eq!(
        refresh.0.load(Ordering::SeqCst),
        3,
        "observation must not call the refresher"
    );
    assert_eq!(
        session.registered_tool_info()[0].spec.description,
        "/configured"
    );
    let mut inventories = Vec::new();
    let mut boundaries = Vec::new();
    while let Ok(event) = events.try_recv() {
        match event {
            SessionEvent::ToolInventory {
                run_id,
                phase,
                tools,
            } => {
                boundaries.push(match phase {
                    ToolInventoryPhase::RequestPreparation => "prepare",
                    ToolInventoryPhase::Dispatch => "dispatch",
                });
                inventories.push((run_id, phase, tools));
            }
            SessionEvent::Request(_) => boundaries.push("request"),
            _ => {}
        }
    }
    assert_eq!(
        boundaries,
        ["prepare", "request", "dispatch", "prepare", "request"]
    );
    assert_eq!(inventories.len(), 3);
    assert!(inventories.iter().all(|entry| entry.0 == inventories[0].0));
    for (index, (phase, description)) in [
        (ToolInventoryPhase::RequestPreparation, "/request"),
        (ToolInventoryPhase::Dispatch, "/dispatch"),
        (ToolInventoryPhase::RequestPreparation, "/dispatch"),
    ]
    .into_iter()
    .enumerate()
    {
        assert_eq!(inventories[index].1, phase);
        let info = &inventories[index].2[0];
        assert_eq!(info.spec.name, "fixture");
        assert_eq!(info.spec.description, description);
        assert_eq!(info.extension_id, "versioned");
        assert_eq!(
            info.extension_source_path.as_deref(),
            Some(Path::new(description))
        );
        assert!(info.active);
        assert_eq!(inventories[index].2.len(), 2);
        let inactive = &inventories[index].2[1];
        assert_eq!(inactive.spec.name, "hidden");
        assert!(!inactive.active);
    }
    let requests = model.0.lock().unwrap();
    assert_eq!(requests[0].tools.len(), 1);
    assert_eq!(requests[0].tools[0].description, "/request");
    assert!(requests[1].messages.iter().any(|m| matches!(m, pi_rs_core::Message::Tool {content, is_error: false, ..} if content == "/dispatch")));
}
struct Fails;
#[async_trait::async_trait]
impl RegistryRefresher for Fails {
    async fn refresh(&self, _: &Registry) -> Result<Option<RegistryUpdate>> {
        Err(Error::Config("refresh failed".into()))
    }
}
#[tokio::test]
async fn failed_refresh_emits_no_selected_inventory() {
    let mut registry = Registry::default();
    registry
        .register(Arc::new(VersionedExtension("/configured")))
        .unwrap();
    registry.set_refresher(Arc::new(Fails));
    let mut session = SessionBuilder::new(Arc::new(Script(Mutex::new(vec![]))), "fixture")
        .registry(registry)
        .in_memory()
        .unwrap();
    let mut events = session.subscribe();
    assert!(session.prompt("test").await.is_err());
    while let Ok(event) = events.try_recv() {
        assert!(!matches!(event, SessionEvent::ToolInventory { .. }));
    }
}

struct FailsOnDispatch(AtomicUsize);
#[async_trait::async_trait]
impl RegistryRefresher for FailsOnDispatch {
    async fn refresh(&self, _: &Registry) -> Result<Option<RegistryUpdate>> {
        if self.0.fetch_add(1, Ordering::SeqCst) == 0 {
            Ok(None)
        } else {
            Err(Error::Config("dispatch refresh failed".into()))
        }
    }
}
#[tokio::test]
async fn failed_dispatch_refresh_does_not_report_dispatch_inventory() {
    let mut registry = Registry::default();
    registry
        .register(Arc::new(VersionedExtension("/configured")))
        .unwrap();
    registry.set_refresher(Arc::new(FailsOnDispatch(AtomicUsize::new(0))));
    let mut session = SessionBuilder::new(Arc::new(Script(Mutex::new(vec![]))), "fixture")
        .registry(registry)
        .in_memory()
        .unwrap();
    let mut events = session.subscribe();
    assert!(session.prompt("test").await.is_err());
    let mut phases = vec![];
    while let Ok(event) = events.try_recv() {
        if let SessionEvent::ToolInventory { phase, .. } = event {
            phases.push(phase);
        }
    }
    assert_eq!(phases, [ToolInventoryPhase::RequestPreparation]);
}
