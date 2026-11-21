use std::path::PathBuf;

use fluent_uri::Iri;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{Error, Result};

/// MCP configuration supplied by an extension. An embedding host decides how to connect it.
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct McpServerDeclaration {
    pub name: String,
    pub config: Value,
}

impl std::fmt::Debug for McpServerDeclaration {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("McpServerDeclaration")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

impl McpServerDeclaration {
    fn validate_and_normalize(&mut self) -> Result<()> {
        if self.name.is_empty()
            || !self
                .name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"_-".contains(&byte))
        {
            return Err(Error::Config(
                "MCP server names use letters, digits, underscore or hyphen".into(),
            ));
        }
        let invalid = || Error::Config(format!("invalid MCP configuration for {:?}", self.name));
        let config = self.config.as_object().ok_or_else(invalid)?;
        let transport = match config.get("type") {
            None => None,
            Some(value) => Some(value.as_str().ok_or_else(invalid)?),
        };
        let command = config.get("command");
        let url = config.get("url");
        let string = |value: &Value| value.is_string();
        let string_list = |value: &Value| {
            value
                .as_array()
                .is_some_and(|items| items.iter().all(string))
        };
        let string_map = |value: &Value| {
            value
                .as_object()
                .is_some_and(|entries| entries.values().all(string))
        };
        let valid = match (transport, command, url) {
            (None | Some("stdio"), Some(command), None) => {
                command.as_str().is_some_and(|text| !text.trim().is_empty())
                    && config.get("args").is_none_or(string_list)
                    && config.get("env").is_none_or(string_map)
                    && config.get("cwd").is_none_or(string)
            }
            (None | Some("http" | "streamable-http"), None, Some(url)) => {
                url.as_str().and_then(http_iri).is_some_and(|url| {
                    config.get("auth").is_none_or(|auth| {
                        auth.get("provider")
                            .and_then(Value::as_str)
                            .is_some_and(|provider| !provider.is_empty())
                            && secure_or_loopback(&url)
                    })
                }) && config.get("headers").is_none_or(string_map)
                    && config.get("oauth").is_none_or(valid_oauth)
            }
            _ => false,
        };
        if !valid
            || !config.get("exposure").is_none_or(valid_exposure)
            || !config.get("toolExposure").is_none_or(|value| {
                value
                    .as_object()
                    .is_some_and(|entries| entries.values().all(valid_exposure))
            })
            || !config.get("enabled").is_none_or(Value::is_boolean)
            || !config.get("description").is_none_or(string)
            || !config
                .get("timeout")
                .is_none_or(|value| value.as_f64().is_some_and(|seconds| seconds > 0.0))
        {
            return Err(invalid());
        }
        if let Some(value) = self.config.get_mut("exposure") {
            normalize_exposure(value);
        }
        if let Some(overrides) = self
            .config
            .get_mut("toolExposure")
            .and_then(Value::as_object_mut)
        {
            for value in overrides.values_mut() {
                normalize_exposure(value);
            }
        }
        Ok(())
    }
}

fn valid_exposure(value: &Value) -> bool {
    matches!(
        value.as_str(),
        Some("codemode" | "codemode-deferred" | "deferred" | "direct" | "hidden" | "off")
    )
}

fn normalize_exposure(value: &mut Value) {
    match value.as_str() {
        Some("codemode-deferred") => *value = "codemode".into(),
        Some("off") => *value = "hidden".into(),
        _ => {}
    }
}

fn http_iri(value: &str) -> Option<Iri<&str>> {
    let url = Iri::parse(value).ok()?;
    let authority = url.authority()?;
    let scheme = url.scheme().as_str();
    ((scheme.eq_ignore_ascii_case("http") || scheme.eq_ignore_ascii_case("https"))
        && !authority.host().is_empty()
        && authority.port_to_u16().is_ok())
    .then_some(url)
}

fn is_loopback(url: &Iri<&str>) -> bool {
    url.authority().is_some_and(|authority| {
        let host = authority.host();
        host.eq_ignore_ascii_case("localhost") || matches!(host, "127.0.0.1" | "[::1]")
    })
}

fn secure_or_loopback(url: &Iri<&str>) -> bool {
    url.scheme().as_str().eq_ignore_ascii_case("https") || is_loopback(url)
}

fn valid_oauth(value: &Value) -> bool {
    if value.is_boolean() {
        return true;
    }
    let Some(config) = value.as_object() else {
        return false;
    };
    let port = match config.get("callbackPort") {
        None => None,
        Some(port) => {
            let Some(port) = port
                .as_u64()
                .and_then(|port| u16::try_from(port).ok())
                .filter(|&port| port != 0)
            else {
                return false;
            };
            Some(port)
        }
    };
    let callback = match config.get("callbackUrl") {
        None => None,
        Some(value) => {
            let Some(url) = value.as_str().and_then(http_iri) else {
                return false;
            };
            if !url.scheme().as_str().eq_ignore_ascii_case("http")
                || !is_loopback(&url)
                || url.query().is_some()
                || url.fragment().is_some()
                || (port.is_some()
                    && url
                        .authority()
                        .and_then(|address| address.port_to_u16().ok().flatten())
                        .is_some_and(|actual| Some(actual) != port))
            {
                return false;
            }
            Some(url)
        }
    };
    if !["clientId", "clientSecret", "scope"]
        .iter()
        .all(|field| config.get(*field).is_none_or(Value::is_string))
        || config
            .get("clientName")
            .is_some_and(|value| value.as_str().is_none_or(|name| name.trim().is_empty()))
        || !config.get("authServerMetadataUrl").is_none_or(|value| {
            value
                .as_str()
                .and_then(http_iri)
                .is_some_and(|url| secure_or_loopback(&url))
        })
    {
        return false;
    }
    match config.get("clientRegistration") {
        None => true,
        Some(value) => match value.as_str() {
            Some("auto" | "dcr") => true,
            Some("cimd") => {
                !config.contains_key("clientId")
                    && !config.contains_key("clientName")
                    && callback.is_none_or(|callback| {
                        callback
                            .authority()
                            .is_some_and(|address| address.host() != "[::1]")
                            && callback.path().as_str() == "/callback"
                    })
            }
            _ => false,
        },
    }
}

/// A declaration and its registered extension owner.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct RegisteredMcpServer {
    #[serde(flatten)]
    pub server: McpServerDeclaration,
    pub extension_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extension_path: Option<PathBuf>,
}

#[derive(Clone, Default)]
pub(crate) struct McpServerRegistrations(Vec<RegisteredMcpServer>);

impl McpServerRegistrations {
    pub(crate) fn servers(&self) -> &[RegisteredMcpServer] {
        &self.0
    }

    pub(crate) fn put(&mut self, mut registration: RegisteredMcpServer) -> Result<()> {
        registration.server.validate_and_normalize()?;
        let name = registration.server.name.as_str();
        let namespace = name.replace('-', "_");
        if let Some(existing) = self
            .0
            .iter_mut()
            .find(|existing| existing.server.name.replace('-', "_") == namespace)
        {
            if existing.server.name != name {
                return Err(Error::Config(format!(
                    "MCP server {name:?} conflicts with registered server {:?}",
                    existing.server.name
                )));
            }
            if existing.extension_id != registration.extension_id {
                return Err(Error::Config(format!(
                    "MCP server {name:?} is already registered by extension {:?}",
                    existing.extension_id
                )));
            }
            *existing = registration;
        } else {
            self.0.push(registration);
        }
        Ok(())
    }

    pub(crate) fn remove(&mut self, owner: &str, name: &str) -> bool {
        let len = self.0.len();
        self.0
            .retain(|entry| entry.extension_id != owner || entry.server.name != name);
        self.0.len() != len
    }

    pub(crate) fn remove_owner(&mut self, owner: &str) {
        self.0.retain(|entry| entry.extension_id != owner);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Extension, Registry};
    use std::sync::Arc;

    struct Declares(&'static str, Vec<McpServerDeclaration>);
    impl Extension for Declares {
        fn id(&self) -> &str {
            self.0
        }
        fn mcp_server_declarations(&self) -> Vec<McpServerDeclaration> {
            self.1.clone()
        }
    }

    fn declaration(name: &str, command: &str) -> McpServerDeclaration {
        McpServerDeclaration {
            name: name.into(),
            config: serde_json::json!({"command":command}),
        }
    }

    #[test]
    fn native_mcp_registration_owns_namespace_and_updates_in_place() {
        let mut registry = Registry::default();
        registry
            .register(Arc::new(Declares(
                "owner",
                vec![declaration("my-server", "first")],
            )))
            .unwrap();
        registry
            .register(Arc::new(Declares(
                "other",
                vec![declaration("other", "second")],
            )))
            .unwrap();

        let clone = registry.clone();
        assert!(
            registry
                .register_mcp_server("other", declaration("my-server", "other"))
                .is_err()
        );
        assert!(
            registry
                .register_mcp_server("owner", declaration("my_server", "alias"))
                .is_err()
        );
        assert!(!registry.unregister_mcp_server("other", "my-server"));
        registry
            .register_mcp_server("owner", declaration("my-server", "updated"))
            .unwrap();
        assert_eq!(
            registry.registered_mcp_servers()[0].server.config["command"],
            "updated"
        );
        assert_eq!(registry.registered_mcp_servers()[1].server.name, "other");
        assert_eq!(
            clone.registered_mcp_servers()[0].server.config["command"],
            "first"
        );
        assert!(registry.unregister_mcp_server("owner", "my-server"));
        assert_eq!(registry.registered_mcp_servers()[0].server.name, "other");
    }

    #[test]
    fn native_mcp_registration_is_atomic_and_keeps_config_out_of_debug() {
        let mut registry = Registry::default();
        let original = Arc::new(Declares("owner", vec![declaration("original", "start")]));
        registry.register(original).unwrap();
        let invalid = McpServerDeclaration {
            name: "broken".into(),
            config: serde_json::json!({"url":"file:///secret","headers":{"Authorization":"secret"}}),
        };
        assert!(!format!("{invalid:?}").contains("secret"));
        assert!(
            registry
                .replace(Arc::new(Declares(
                    "owner",
                    vec![declaration("new", "start"), invalid]
                )))
                .is_err()
        );
        assert_eq!(registry.registered_mcp_servers().len(), 1);
        assert_eq!(registry.registered_mcp_servers()[0].server.name, "original");
    }
    #[test]
    fn native_mcp_registration_validates_url_oauth_and_exposure_before_storage() {
        let mut registry = Registry::default();
        registry
            .register(Arc::new(Declares("owner", vec![])))
            .unwrap();
        for config in [
            serde_json::json!({"url":"https://localhost:65536"}),
            serde_json::json!({"url":"https://[invalid]"}),
            serde_json::json!({"url":"http://remote.invalid", "auth":{"provider":"fixture"}}),
            serde_json::json!({"url":"https://remote.invalid", "oauth":{"callbackUrl":"https://localhost/callback"}}),
            serde_json::json!({"url":"https://remote.invalid", "oauth":{"callbackPort":1000,"callbackUrl":"http://localhost:1001/callback"}}),
            serde_json::json!({"command":"fixture", "toolExposure":{"read":"invalid"}}),
        ] {
            assert!(
                registry
                    .register_mcp_server(
                        "owner",
                        McpServerDeclaration {
                            name: "fixture".into(),
                            config
                        }
                    )
                    .is_err()
            );
        }
        assert!(registry.registered_mcp_servers().is_empty());
        registry.register_mcp_server("owner", McpServerDeclaration {
            name: "fixture".into(), config: serde_json::json!({
                "url":"https://example.invalid", "exposure":"codemode-deferred", "toolExposure":{"read":"off"},
            }),
        }).unwrap();
        let config = &registry.registered_mcp_servers()[0].server.config;
        assert_eq!(config["exposure"], "codemode");
        assert_eq!(config["toolExposure"]["read"], "hidden");
    }

    #[test]
    fn native_mcp_registration_can_be_disabled_before_validating_declarations() {
        let invalid = McpServerDeclaration {
            name: "fixture".into(),
            config: serde_json::json!({"url":"invalid"}),
        };
        let extension: Arc<dyn Extension> = Arc::new(Declares("owner", vec![invalid]));
        assert!(Registry::compose(vec![extension.clone()], &Default::default()).is_err());
        let mut registry =
            Registry::compose_with_mcp_declarations(vec![extension], &Default::default(), false)
                .unwrap();
        assert!(registry.registered_mcp_servers().is_empty());
        assert!(
            registry
                .register_mcp_server("owner", declaration("valid", "fixture"))
                .is_err()
        );
    }

    #[tokio::test]
    async fn native_mcp_registration_sdk_emits_post_change_snapshots_only() {
        struct NoModel;
        #[async_trait::async_trait]
        impl crate::Model for NoModel {
            async fn stream(&self, _: crate::ModelRequest) -> Result<crate::ModelStream> {
                panic!("changing the registry must not run the model");
            }
        }

        struct Observe(std::sync::Mutex<Vec<Vec<String>>>);
        #[async_trait::async_trait]
        impl Extension for Observe {
            fn id(&self) -> &str {
                "observe"
            }
            fn handles_mcp_servers_change(&self) -> bool {
                true
            }
            async fn lifecycle(
                &self,
                event: &crate::LifecycleEvent,
                _: Option<&crate::ExtensionState>,
            ) -> crate::ExtensionResult<()> {
                if let crate::LifecycleEvent::McpServersChange { servers } = event {
                    self.0.lock().unwrap().push(
                        servers
                            .iter()
                            .map(|server| server.server.name.clone())
                            .collect(),
                    );
                }
                Ok(())
            }
        }

        let observe = Arc::new(Observe(Default::default()));
        let mut session = crate::SessionBuilder::new(Arc::new(NoModel), "fixture")
            .extension(Arc::new(Declares(
                "owner",
                vec![declaration("first", "fixture")],
            )))
            .unwrap()
            .extension(observe.clone())
            .unwrap()
            .in_memory()
            .unwrap();
        assert_eq!(session.registered_mcp_servers()[0].server.name, "first");
        assert!(observe.0.lock().unwrap().is_empty());
        assert!(
            session
                .register_mcp_server("missing", declaration("unknown", "fixture"))
                .await
                .is_err()
        );
        assert!(
            !session
                .unregister_mcp_server("observe", "first")
                .await
                .unwrap()
        );
        session
            .register_mcp_server("owner", declaration("second", "fixture"))
            .await
            .unwrap();
        assert!(
            session
                .unregister_mcp_server("owner", "first")
                .await
                .unwrap()
        );
        assert_eq!(
            *observe.0.lock().unwrap(),
            [vec!["first", "second"], vec!["second"]]
        );
    }
}
