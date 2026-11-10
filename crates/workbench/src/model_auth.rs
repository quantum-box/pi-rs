//! Private API keys for native model providers.
use std::collections::BTreeMap;
use std::sync::{Arc, LazyLock, Mutex};

use pi_rs::{Error, Result};
use serde_json::{Value, json};

use super::{Provider, config};

#[derive(Clone)]
pub(super) enum Credential {
    Literal(String),
    Command(String),
    StoredCommand(String),
}

type CommandResult = std::result::Result<String, &'static str>;
type CachedCommand = Arc<tokio::sync::OnceCell<CommandResult>>;
static STORED_COMMANDS: LazyLock<Mutex<BTreeMap<String, CachedCommand>>> =
    LazyLock::new(|| Mutex::new(BTreeMap::new()));

impl Credential {
    pub fn configured(value: &str) -> Option<Self> {
        if let Some(command) = value.strip_prefix('!') {
            return (!command.trim().is_empty() && value.len() <= 16 * 1024)
                .then(|| Self::Command(value.into()));
        }
        super::model_profiles::expand_value(value)
            .filter(|value| !value.trim().is_empty())
            .map(Self::Literal)
    }

    pub fn literal(&self) -> Option<&str> {
        match self {
            Self::Literal(value) => Some(value),
            Self::Command(_) | Self::StoredCommand(_) => None,
        }
    }

    pub fn is_command(&self) -> bool {
        self.literal().is_none()
    }

    pub async fn resolve(&self) -> Result<String> {
        let value = match self {
            Self::Literal(value) => return Ok(value.clone()),
            Self::Command(command) => resolve_command(command).await,
            Self::StoredCommand(command) => {
                let cell = STORED_COMMANDS
                    .lock()
                    .expect("stored credential commands")
                    .entry(command.clone())
                    .or_default()
                    .clone();
                cell.get_or_init(|| resolve_command(command)).await.clone()
            }
        };
        value.map_err(|message| Error::Config(message.into()))
    }
}

async fn resolve_command(command: &str) -> CommandResult {
    let value = pi_rs_workbench::mcp::resolve_value(command)
        .await
        .map_err(|_| "Model credential command failed")?;
    if !valid_key(&value) {
        return Err("Model credential command returned an invalid key");
    }
    Ok(value)
}

#[derive(Clone, Default)]
pub(super) struct Keys {
    entries: BTreeMap<String, SavedKey>,
}

#[derive(Clone)]
struct SavedKey {
    endpoint: String,
    key: Credential,
}

impl Keys {
    pub fn load() -> Result<Self> {
        let value = config::read_object(&config::agent_dir().join("auth.json"))?;
        Ok(Self::parse(&value))
    }

    fn parse(value: &Value) -> Self {
        let entries = value
            .as_object()
            .into_iter()
            .flatten()
            .filter_map(|(name, entry)| {
                if entry["type"] != "api_key" {
                    return None;
                }
                let value = entry["key"].as_str()?;
                let key = if entry["literal"] == true {
                    valid_key(value).then(|| Credential::Literal(value.into()))?
                } else {
                    match Credential::configured(value)? {
                        Credential::Command(command) => Credential::StoredCommand(command),
                        Credential::Literal(value) if valid_key(&value) => {
                            Credential::Literal(value)
                        }
                        _ => return None,
                    }
                };
                let endpoint = match entry.get("endpoint") {
                    None => Provider::named(name)
                        .map(|provider| provider.default_endpoint())
                        .or_else(|| {
                            super::operation_models::official_base(&name.to_ascii_lowercase())
                        })?,
                    Some(value) => value.as_str()?,
                };
                Some((
                    name.to_ascii_lowercase(),
                    SavedKey {
                        endpoint: endpoint.to_owned(),
                        key,
                    },
                ))
            })
            .collect();
        Self { entries }
    }

    pub fn credential(&self, provider: &Provider, endpoint: &str) -> Option<&Credential> {
        let entry = self.entries.get(provider.id())?;
        let legacy_radius = provider == &Provider::Radius
            && endpoint == super::radius_auth::DEFAULT_MODEL_ENDPOINT
            && entry.endpoint == "https://radius.pi.dev/messages";
        (entry.endpoint == endpoint || legacy_radius).then_some(&entry.key)
    }

    pub fn operation_credential(
        &self,
        provider: &str,
        endpoint: &str,
        official: bool,
    ) -> Option<&Credential> {
        let entry = self.entries.get(provider)?;
        (entry.endpoint == endpoint
            || (official
                && super::operation_models::official_base(provider)
                    == Some(entry.endpoint.as_str())))
        .then_some(&entry.key)
    }

    #[cfg(test)]
    fn get(&self, provider: &Provider, endpoint: &str) -> Option<&str> {
        self.credential(provider, endpoint)?.literal()
    }

    pub fn has(&self, provider: &Provider) -> bool {
        self.entries.contains_key(provider.id())
    }
}

pub(super) fn save(provider: &Provider, endpoint: &str, key: &str) -> Result<()> {
    let key = key.trim();
    if !valid_key(key) {
        return Err(Error::Config(
            "API key must contain 1–4096 printable ASCII characters without spaces".into(),
        ));
    }
    config::update_object(&config::agent_dir().join("auth.json"), |data| {
        data.insert(
            provider.id().into(),
            json!({"type":"api_key","key":key,"endpoint":endpoint,"literal":true}),
        );
        Ok(())
    })
}

pub(super) fn remove(provider: &Provider) -> Result<bool> {
    let mut removed = false;
    config::update_object(&config::agent_dir().join("auth.json"), |data| {
        if data
            .get(provider.id())
            .and_then(|value| value.get("type"))
            .and_then(Value::as_str)
            == Some("api_key")
        {
            data.remove(provider.id());
            removed = true;
        }
        Ok(())
    })?;
    Ok(removed)
}

pub(super) fn valid_key(key: &str) -> bool {
    !key.is_empty() && key.len() <= 4096 && key.bytes().all(|byte| byte.is_ascii_graphic())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    #[tokio::test]
    async fn stored_commands_cache_results_and_profile_commands_read_again() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("key");
        let command = format!("!cat {}", shell_words::quote(path.to_str().unwrap()));
        std::fs::write(&path, "first").unwrap();
        let stored = Credential::StoredCommand(command.clone());
        assert_eq!(stored.resolve().await.unwrap(), "first");
        std::fs::write(&path, "second").unwrap();
        assert_eq!(stored.resolve().await.unwrap(), "first");
        assert_eq!(
            Credential::Command(command).resolve().await.unwrap(),
            "second"
        );

        let missing = directory.path().join("missing");
        let command = Credential::StoredCommand(format!(
            "!cat {}",
            shell_words::quote(missing.to_str().unwrap())
        ));
        assert!(command.resolve().await.is_err());
        std::fs::write(missing, "available").unwrap();
        assert!(command.resolve().await.is_err());
    }

    #[test]
    fn saved_keys_only_match_their_bound_endpoint() {
        let proxy = "http://127.0.0.1:8000/v1/chat/completions";
        let keys = Keys::parse(&json!({
            "ollama": {"type":"api_key","key":"proxy-key","endpoint":proxy},
            "anthropic": {"type":"api_key","key":"legacy-key"},
        }));
        assert_eq!(keys.get(&Provider::Ollama, proxy), Some("proxy-key"));
        assert!(
            keys.get(&Provider::Ollama, Provider::Ollama.default_endpoint())
                .is_none()
        );
        assert_eq!(
            keys.get(&Provider::Anthropic, Provider::Anthropic.default_endpoint()),
            Some("legacy-key")
        );
        assert!(
            keys.get(&Provider::Anthropic, "http://127.0.0.1:8001/v1/messages")
                .is_none()
        );
    }
}
