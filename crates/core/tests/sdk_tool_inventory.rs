use pi_rs_core::{
    Extension, Model, ModelRequest, ModelStream, Outcome, Registry, Result, SessionBuilder, Tool,
    ToolContext, ToolSpec,
};
use serde_json::{Value, json};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

struct FixtureExtension {
    id: &'static str,
    names: Vec<&'static str>,
    source: Option<PathBuf>,
}
impl Extension for FixtureExtension {
    fn id(&self) -> &str {
        self.id
    }
    fn command_source(&self) -> Option<&Path> {
        self.source.as_deref()
    }
    fn tools(&self) -> Vec<Arc<dyn Tool>> {
        self.names
            .iter()
            .map(|&name| Arc::new(FixtureTool(name)) as Arc<dyn Tool>)
            .collect()
    }
}
struct FixtureTool(&'static str);
#[async_trait::async_trait]
impl Tool for FixtureTool {
    fn spec(&self) -> ToolSpec {
        ToolSpec {
            name: self.0.into(),
            description: format!("description for {}", self.0),
            parameters: json!({"type":"object"}),
        }
    }
    async fn invoke(&self, _: Value, _: ToolContext) -> Outcome {
        Outcome::Succeeded("ok".into())
    }
}
struct UnusedModel;
#[async_trait::async_trait]
impl Model for UnusedModel {
    async fn stream(&self, _: ModelRequest) -> Result<ModelStream> {
        unreachable!()
    }
}
fn extension(
    id: &'static str,
    names: Vec<&'static str>,
    source: Option<&str>,
) -> Arc<dyn Extension> {
    Arc::new(FixtureExtension {
        id,
        names,
        source: source.map(PathBuf::from),
    })
}

#[tokio::test]
async fn registry_and_sdk_report_owners_sources_and_active_selection() {
    let mut registry = Registry::default();
    registry
        .register(extension(
            "remote",
            vec!["zeta", "alpha"],
            Some("/extension/path"),
        ))
        .unwrap();
    registry
        .register(extension("builtin", vec!["beta"], None))
        .unwrap();
    registry
        .set_active_tools(["alpha".into(), "beta".into()])
        .unwrap();
    let info = registry.registered_tool_info();
    assert_eq!(
        info.iter()
            .map(|item| item.spec.name.as_str())
            .collect::<Vec<_>>(),
        ["alpha", "beta", "zeta"]
    );
    assert_eq!(info[0].extension_id, "remote");
    assert_eq!(
        info[0].extension_source_path.as_deref(),
        Some(Path::new("/extension/path"))
    );
    assert_eq!(info[0].spec.description, "description for alpha");
    assert!(info[0].active && info[1].active && !info[2].active);
    assert_eq!(info[1].extension_id, "builtin");
    assert!(info[1].extension_source_path.is_none());
    assert!(registry.has_tool("zeta"));
    assert_eq!(
        registry
            .active_tool_specs()
            .iter()
            .map(|tool| tool.name.as_str())
            .collect::<Vec<_>>(),
        ["alpha", "beta"]
    );
    assert!(registry.set_active_tools(["missing".into()]).is_err());
    assert_eq!(registry.registered_tool_info(), info);
    let session = SessionBuilder::new(Arc::new(UnusedModel), "fixture")
        .registry(registry)
        .in_memory()
        .unwrap();
    assert_eq!(session.registered_tool_info(), info);
    assert_eq!(session.tools().len(), 3);
}

#[test]
fn metadata_tracks_replacement_and_retain_without_stale_owners() {
    let mut registry = Registry::default();
    registry
        .register(extension("first", vec!["old"], Some("/old")))
        .unwrap();
    registry
        .register(extension("other", vec!["keep"], None))
        .unwrap();
    registry
        .set_active_tools(["old".into(), "keep".into()])
        .unwrap();
    assert!(
        registry
            .replace(extension("first", vec!["new"], Some("/new")))
            .unwrap()
    );
    let info = registry.registered_tool_info();
    assert_eq!(info.len(), 2);
    assert_eq!(info[1].spec.name, "new");
    assert_eq!(info[1].extension_id, "first");
    assert_eq!(
        info[1].extension_source_path.as_deref(),
        Some(Path::new("/new"))
    );
    assert!(!info[1].active);
    assert!(info[0].active);
    registry.retain_tools(|tool| tool.name == "new");
    assert_eq!(registry.registered_tool_info().len(), 1);
    assert_eq!(registry.registered_tool_info()[0].spec.name, "new");
}
