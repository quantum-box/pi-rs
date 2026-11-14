use pi_rs_core::{Extension, ExtensionCommandSourceKind, Registry};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

struct CommandExtension {
    id: &'static str,
    builtin: bool,
    path: Option<PathBuf>,
}
impl Extension for CommandExtension {
    fn id(&self) -> &str {
        self.id
    }
    fn commands(&self) -> &[&str] {
        &["shared"]
    }
    fn command_source(&self) -> Option<&Path> {
        self.path.as_deref()
    }
    fn source_is_builtin(&self) -> bool {
        self.builtin
    }
}
#[test]
fn command_origins_follow_the_owner_through_collisions_and_replacement() {
    let mut registry = Registry::default();
    registry
        .register(Arc::new(CommandExtension {
            id: "native",
            builtin: true,
            path: None,
        }))
        .unwrap();
    registry
        .register(Arc::new(CommandExtension {
            id: "inline",
            builtin: false,
            path: None,
        }))
        .unwrap();
    registry
        .register(Arc::new(CommandExtension {
            id: "external",
            builtin: false,
            path: Some("/binary".into()),
        }))
        .unwrap();
    let commands = registry.registered_commands();
    assert_eq!(
        commands
            .iter()
            .map(|c| c.invocation_name.as_str())
            .collect::<Vec<_>>(),
        ["shared:1", "shared:2", "shared:3"]
    );
    assert_eq!(
        commands.iter().map(|c| c.source_kind).collect::<Vec<_>>(),
        [
            ExtensionCommandSourceKind::Builtin,
            ExtensionCommandSourceKind::Inline,
            ExtensionCommandSourceKind::File
        ]
    );
    assert_eq!(
        commands[2].source_path.as_deref(),
        Some(Path::new("/binary"))
    );
    assert!(
        registry
            .replace(Arc::new(CommandExtension {
                id: "inline",
                builtin: false,
                path: Some("/replacement".into())
            }))
            .unwrap()
    );
    let commands = registry.registered_commands();
    assert_eq!(commands[1].extension_id, "inline");
    assert_eq!(commands[1].source_kind, ExtensionCommandSourceKind::File);
    assert_eq!(
        commands[1].source_path.as_deref(),
        Some(Path::new("/replacement"))
    );
}
