use pi_rs_core::{
    Extension, ExtensionCommandSourceKind, ExtensionResult, ExtensionState, Model, ModelRequest,
    ModelStream, Result, SessionBuilder,
};
use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

struct CommandExtension {
    id: &'static str,
    source: Option<PathBuf>,
    builtin: bool,
    observed: Arc<Mutex<Vec<String>>>,
}
impl Extension for CommandExtension {
    fn id(&self) -> &str {
        self.id
    }
    fn command_names(&self) -> Vec<&str> {
        vec!["inspect"]
    }
    fn command_description(&self, _: &str) -> Option<&str> {
        Some("Inspect a resource")
    }
    fn command_source(&self) -> Option<&Path> {
        self.source.as_deref()
    }
    fn command(
        &self,
        name: &str,
        arguments: &str,
        _: Option<&ExtensionState>,
    ) -> ExtensionResult<ExtensionState> {
        self.observed
            .lock()
            .unwrap()
            .push(format!("{}:{name}:{arguments}", self.id));
        Ok(ExtensionState {
            version: 1,
            data: serde_json::Value::Null,
        })
    }
    fn source_is_builtin(&self) -> bool {
        self.builtin
    }
}
struct UnusedModel;
#[async_trait::async_trait]
impl Model for UnusedModel {
    async fn stream(&self, _: ModelRequest) -> Result<ModelStream> {
        unreachable!()
    }
}

#[tokio::test]
async fn owned_session_exposes_command_collisions_and_provenance() {
    let observed = Arc::new(Mutex::new(Vec::new()));
    let first = Arc::new(CommandExtension {
        id: "inline",
        source: None,
        builtin: false,
        observed: observed.clone(),
    });
    let second = Arc::new(CommandExtension {
        id: "file",
        source: Some(PathBuf::from("/fixture/extension")),
        builtin: false,
        observed: observed.clone(),
    });
    let mut session = SessionBuilder::new(Arc::new(UnusedModel), "fixture")
        .extension(first)
        .unwrap()
        .in_memory()
        .unwrap();
    let initial = session.registered_commands();
    assert_eq!(initial[0].invocation_name, "inspect");
    assert_eq!(initial[0].source_kind, ExtensionCommandSourceKind::Inline);
    session.register_extension(second).unwrap();
    let commands = session.registered_commands();
    assert_eq!(
        commands
            .iter()
            .map(|command| command.invocation_name.as_str())
            .collect::<Vec<_>>(),
        ["inspect:1", "inspect:2"]
    );
    assert_eq!(commands[1].source_kind, ExtensionCommandSourceKind::File);
    assert_eq!(
        commands[1].source_path.as_deref(),
        Some("/fixture/extension")
    );
    assert_eq!(
        commands[1].description.as_deref(),
        Some("Inspect a resource")
    );
    let encoded = serde_json::to_value(&commands[1]).unwrap();
    assert_eq!(encoded["source_kind"], "file");
    assert_eq!(encoded["invocation_name"], "inspect:2");
    assert!(
        session
            .invoke_registered_command("inspect:2", "arg")
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(*observed.lock().unwrap(), ["file:inspect:arg"]);
    assert!(
        session
            .invoke_registered_command("inspect", "missing")
            .await
            .is_err()
    );
    session.close().await.unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn non_utf8_source_paths_are_serializable_display_text() {
    use std::os::unix::ffi::OsStringExt;
    let source = PathBuf::from(std::ffi::OsString::from_vec(b"/fixture/\xff".to_vec()));
    let extension = Arc::new(CommandExtension {
        id: "file",
        source: Some(source),
        builtin: false,
        observed: Arc::default(),
    });
    let mut session = SessionBuilder::new(Arc::new(UnusedModel), "fixture")
        .extension(extension)
        .unwrap()
        .in_memory()
        .unwrap();
    let commands = session.registered_commands();
    let encoded = serde_json::to_value(&commands).unwrap();
    assert!(
        encoded[0]["source_path"]
            .as_str()
            .unwrap()
            .contains('\u{fffd}')
    );
    session.close().await.unwrap();
}
