//! Explicit extension composition for native SDK hosts.
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Component, PathBuf};
use std::sync::Arc;

use pi_rs_core::{Error, Extension, ExtensionFlagValue, Registry, Result};

use crate::ExecutableExtension;

/// A host-supplied extension or a file the host authorizes Pi to execute.
/// Executable grants include symlink targets and authorize discovery callbacks.
#[derive(Clone)]
pub enum NativeExtensionSource {
    Inline(Arc<dyn Extension>),
    /// Omitted when any enabled, non-replaceable extension registers a tool,
    /// command or flag with the same name.
    ReplaceableInline(Arc<dyn Extension>),
    Executable {
        path: PathBuf,
        flags: BTreeMap<String, ExtensionFlagValue>,
    },
}

/// Compose a fresh SDK registry from the supplied sources. Host-controlled
/// order is preserved among extensions whose dependencies are already
/// satisfied. Built-ins, settings and packages are discovered by the caller.
#[derive(Clone, Default)]
pub struct NativeExtensionLoader {
    pub sources: Vec<NativeExtensionSource>,
    pub disabled: BTreeSet<String>,
}

impl NativeExtensionLoader {
    /// Resolve all descriptors and flags before publishing a registry. Every
    /// executable path must be absolute without parent components. An error
    /// leaves earlier registries and sessions unchanged. Describing an executable
    /// can have external effects even if its discovered ID is later disabled.
    /// Omit the executable source to prevent discovery.
    pub async fn load(&self) -> Result<Registry> {
        for source in &self.sources {
            if let NativeExtensionSource::Executable { path, .. } = source
                && (!path.is_absolute()
                    || path.components().any(|part| part == Component::ParentDir))
            {
                return Err(Error::Config(format!(
                    "extension executable grant must be absolute without parent components: {}",
                    path.display()
                )));
            }
        }
        let mut extensions = Vec::with_capacity(self.sources.len());
        for source in &self.sources {
            match source {
                NativeExtensionSource::Inline(extension) => {
                    extensions.push((extension.clone(), false))
                }
                NativeExtensionSource::ReplaceableInline(extension) => {
                    extensions.push((extension.clone(), true))
                }
                NativeExtensionSource::Executable { path, flags } => {
                    let mut extension = ExecutableExtension::load_async(path).await?;
                    let mut flags = flags.clone();
                    extension.apply_flags(&mut flags)?;
                    if let Some(name) = flags.keys().next() {
                        return Err(Error::Config(format!(
                            "extension {} does not declare flag {name}",
                            extension.id()
                        )));
                    }
                    extensions.push((Arc::new(extension), false));
                }
            }
        }
        let claimed = if extensions.iter().any(|(_, replaceable)| *replaceable) {
            extensions
                .iter()
                .filter(|(extension, replaceable)| {
                    !replaceable && !self.disabled.contains(extension.id())
                })
                .flat_map(|(extension, _)| registrations(extension.as_ref()))
                .collect()
        } else {
            BTreeSet::new()
        };
        let extensions = extensions
            .into_iter()
            .filter_map(|(extension, replaceable)| {
                let replaced = replaceable
                    && !self.disabled.contains(extension.id())
                    && registrations(extension.as_ref()).any(|name| claimed.contains(&name));
                (!replaced).then_some(extension)
            })
            .collect();
        Registry::compose(extensions, &self.disabled)
    }
}

#[derive(PartialEq, Eq, PartialOrd, Ord)]
enum Registration {
    Tool(String),
    Command(String),
    Flag(String),
}

fn registrations(extension: &dyn Extension) -> impl Iterator<Item = Registration> + '_ {
    extension
        .tools()
        .into_iter()
        .map(|tool| Registration::Tool(tool.spec().name))
        .chain(
            extension
                .command_names()
                .into_iter()
                .map(|name| Registration::Command(name.into())),
        )
        .chain(
            extension
                .cli_flags()
                .into_iter()
                .map(|flag| Registration::Flag(flag.name)),
        )
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Inline(&'static str, &'static [&'static str]);

    impl Extension for Inline {
        fn id(&self) -> &str {
            self.0
        }
        fn dependencies(&self) -> &[&str] {
            self.1
        }
        fn commands(&self) -> &[&str] {
            &["check"]
        }
    }

    #[tokio::test]
    async fn loader_preserves_registry_order_and_existing_snapshot_on_failure() {
        let mut loader = NativeExtensionLoader {
            sources: vec![
                NativeExtensionSource::Inline(Arc::new(Inline("dependent", &["base"]))),
                NativeExtensionSource::Inline(Arc::new(Inline("base", &[]))),
            ],
            ..Default::default()
        };
        let registry = loader.load().await.unwrap();
        let names = registry
            .registered_commands()
            .into_iter()
            .map(|command| command.extension_id)
            .collect::<Vec<_>>();
        assert_eq!(names, ["base", "dependent"]);

        loader.disabled.insert("base".into());
        assert!(loader.load().await.is_err());
        assert_eq!(registry.registered_commands().len(), 2);
    }

    #[tokio::test]
    async fn enabled_non_replaceable_extension_replaces_matching_inline_registration() {
        let mut loader = NativeExtensionLoader {
            sources: vec![
                NativeExtensionSource::ReplaceableInline(Arc::new(Inline("default-check", &[]))),
                NativeExtensionSource::Inline(Arc::new(Inline("custom-check", &[]))),
            ],
            ..Default::default()
        };
        let loaded = loader.load().await.unwrap().registered_commands();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].extension_id, "custom-check");

        loader.disabled.insert("custom-check".into());
        let loaded = loader.load().await.unwrap().registered_commands();
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].extension_id, "default-check");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn waits_for_described_executables_and_applies_only_their_flags() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("fixture");
        std::fs::write(&path, r#"#!/bin/sh
IFS= read -r request
printf '%s\n' '{"version":1,"response":{"Ok":{"id":"fixture","dependencies":[],"commands":["check"],"tools":[]}}}'
"#).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        let mut loader = NativeExtensionLoader {
            sources: vec![NativeExtensionSource::Executable {
                path: path.clone(),
                flags: BTreeMap::new(),
            }],
            ..Default::default()
        };
        let registry = loader.load().await.unwrap();
        let commands = registry.registered_commands();
        assert_eq!(commands[0].source_path, Some(path.canonicalize().unwrap()));

        loader.sources[0] = NativeExtensionSource::Executable {
            path,
            flags: BTreeMap::from([("unknown".into(), ExtensionFlagValue::Boolean(true))]),
        };
        let error = match loader.load().await {
            Ok(_) => panic!("an unknown flag was accepted"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("does not declare flag unknown"));
        loader.sources.push(NativeExtensionSource::Executable {
            path: PathBuf::from("relative"),
            flags: BTreeMap::new(),
        });
        let error = match loader.load().await {
            Ok(_) => panic!("a relative executable was accepted"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("grant must be absolute"));
    }
}
