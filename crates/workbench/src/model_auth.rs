//! Private API keys for native model providers.
use std::collections::BTreeMap;
use std::sync::{Arc, LazyLock, Mutex};

use pi_rs::{Error, Result};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use super::{Provider, config};

#[derive(Clone)]
pub(super) enum Credential {
    Literal(String),
    Bearer(String),
    Command(String),
    StoredCommand(String),
    ChatGpt,
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
            Self::Literal(value) | Self::Bearer(value) => Some(value),
            Self::Command(_) | Self::StoredCommand(_) | Self::ChatGpt => None,
        }
    }

    pub fn is_bearer(&self) -> bool {
        matches!(self, Self::Bearer(_) | Self::ChatGpt)
    }

    pub fn is_command(&self) -> bool {
        matches!(self, Self::Command(_) | Self::StoredCommand(_))
    }

    pub fn requires_resolution(&self) -> bool {
        self.is_command() || matches!(self, Self::ChatGpt)
    }

    pub async fn resolve(&self) -> Result<String> {
        let value = match self {
            Self::Literal(value) | Self::Bearer(value) => return Ok(value.clone()),
            Self::ChatGpt => {
                let settings = super::config::Settings::load(&std::env::current_dir()?, false)?;
                return super::chatgpt_auth::access(&settings).await;
            }
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
    revision: String,
}

#[derive(Clone)]
struct SavedKey {
    endpoint: Option<String>,
    official_routes: bool,
    oauth: bool,
    key: Credential,
}

impl Keys {
    pub fn load() -> Result<Self> {
        let value = config::read_object(&config::agent_dir().join("auth.json"))?;
        Ok(Self::parse(&value))
    }

    pub(super) fn parse(value: &Value) -> Self {
        let entries = value
            .as_object()
            .into_iter()
            .flatten()
            .filter_map(|(name, entry)| {
                let chatgpt =
                    name.eq_ignore_ascii_case("openai-chatgpt") && entry["type"] == "oauth";
                let oauth = (name.eq_ignore_ascii_case("openrouter") || chatgpt)
                    && entry["type"] == "oauth";
                let key = if chatgpt {
                    serde_json::from_value::<pi_rs::openai::oauth::Credential>(
                        entry["credential"].clone(),
                    )
                    .ok()?;
                    Credential::ChatGpt
                } else if oauth {
                    let access = entry["access"].as_str()?;
                    valid_key(access).then(|| Credential::Literal(access.into()))?
                } else {
                    if entry["type"] != "api_key" {
                        return None;
                    }
                    let value = entry["key"].as_str()?;
                    if entry["literal"] == true {
                        valid_key(value).then(|| Credential::Literal(value.into()))?
                    } else {
                        match Credential::configured(value)? {
                            Credential::Command(command) => Credential::StoredCommand(command),
                            Credential::Literal(value) if valid_key(&value) => {
                                Credential::Literal(value)
                            }
                            _ => return None,
                        }
                    }
                };
                let endpoint = match entry.get("endpoint") {
                    None => Provider::named(name)
                        .map(|provider| provider.default_endpoint())
                        .or_else(|| {
                            super::operation_models::official_base(&name.to_ascii_lowercase())
                        }),
                    Some(value) => Some(value.as_str()?),
                };
                if oauth
                    && !chatgpt
                    && super::builtin_chat::find("openrouter")
                        .is_none_or(|builtin| Some(builtin.endpoint()) != endpoint)
                {
                    return None;
                }
                Some((
                    name.to_ascii_lowercase(),
                    SavedKey {
                        endpoint: endpoint.map(str::to_owned),
                        official_routes: oauth || entry.get("endpoint").is_none(),
                        oauth,
                        key,
                    },
                ))
            })
            .collect();
        Self {
            entries,
            revision: format!("{:x}", Sha256::digest(value.to_string())),
        }
    }

    pub fn revision(&self) -> &str {
        &self.revision
    }

    pub fn credential(&self, provider: &Provider, endpoint: &str) -> Option<&Credential> {
        let entry = self.entries.get(provider.id())?;
        let legacy_radius = provider == &Provider::Radius
            && endpoint == super::radius_auth::DEFAULT_MODEL_ENDPOINT
            && entry.endpoint.as_deref() == Some("https://radius.pi.dev/messages");
        let builtin_route = entry.official_routes
            && super::builtin_chat::find(provider.id())
                .is_some_and(|builtin| builtin.is_official_endpoint(endpoint));
        (entry.endpoint.as_deref() == Some(endpoint) || legacy_radius || builtin_route)
            .then_some(&entry.key)
    }

    pub fn custom_credential(
        &self,
        provider: &Provider,
        endpoint: &str,
        profiles: &super::model_profiles::Profiles,
    ) -> Option<&Credential> {
        let entry = self.entries.get(provider.id())?;
        (entry.endpoint.is_none() && profiles.has_endpoint(provider, endpoint))
            .then_some(&entry.key)
    }

    pub fn oauth(&self, provider: &Provider, endpoint: &str) -> bool {
        self.credential(provider, endpoint).is_some()
            && self
                .entries
                .get(provider.id())
                .is_some_and(|entry| entry.oauth)
    }

    pub fn operation_credential(
        &self,
        provider: &str,
        endpoint: &str,
        official: bool,
    ) -> Option<&Credential> {
        let entry = self.entries.get(provider)?;
        (entry.endpoint.as_deref() == Some(endpoint)
            || (official
                && ((entry.endpoint.is_some()
                    && super::operation_models::official_base(provider)
                        == entry.endpoint.as_deref())
                    || (entry.official_routes && super::builtin_chat::find(provider).is_some()))))
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

pub(super) fn save_openrouter_oauth(key: &str) -> Result<()> {
    let provider = Provider::named("openrouter").expect("built-in OpenRouter provider");
    config::update_object(&config::agent_dir().join("auth.json"), |data| {
        data.insert(
            provider.id().into(),
            json!({
                "type":"oauth","access":key,"refresh":"","expires":9_007_199_254_740_991_u64,
                "endpoint":provider.default_endpoint(),
            }),
        );
        Ok(())
    })
}

pub(super) fn remove(provider: &Provider) -> Result<bool> {
    let mut removed = false;
    config::update_object(&config::agent_dir().join("auth.json"), |data| {
        let kind = data
            .get(provider.id())
            .and_then(|value| value["type"].as_str());
        if kind == Some("api_key") || (provider.id() == "openrouter" && kind == Some("oauth")) {
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

    #[test]
    fn chatgpt_oauth_credentials_are_bound_to_the_official_endpoint() {
        let credential = pi_rs::openai::oauth::Credential {
            client_id: "client".into(),
            subject: "subject".into(),
            email: None,
            id_token: "id".into(),
            access_token: "access".into(),
            refresh_token: "refresh".into(),
            expires_at: 100,
            scopes: vec!["chatgpt.tokens.use.direct".into()],
        };
        let keys = Keys::parse(&json!({"openai-chatgpt":{"type":"oauth","credential":credential}}));
        let provider = Provider::named("openai-chatgpt").unwrap();
        assert!(matches!(
            keys.credential(&provider, pi_rs::openai::DEFAULT_ENDPOINT),
            Some(Credential::ChatGpt)
        ));
        assert!(
            keys.credential(&provider, "http://127.0.0.1:1234/v1/responses")
                .is_none()
        );
    }

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

    #[test]
    fn openrouter_oauth_access_is_limited_to_official_routes() {
        let provider = Provider::named("openrouter").unwrap();
        let proxy = "http://127.0.0.1:8000/v1/chat/completions";
        let official = provider.default_endpoint();
        let keys = Keys::parse(
            &json!({"openrouter":{"type":"oauth","access":"fixture-key","endpoint":official}}),
        );
        assert_eq!(keys.get(&provider, official), Some("fixture-key"));
        assert!(keys.get(&provider, proxy).is_none());
        assert_eq!(
            keys.operation_credential(
                "openrouter",
                "https://openrouter.ai/api/v1/classifications",
                true
            )
            .and_then(Credential::literal),
            Some("fixture-key")
        );
        assert!(
            keys.operation_credential("openrouter", proxy, false)
                .is_none()
        );
        let invalid = Keys::parse(
            &json!({"openrouter":{"type":"oauth","access":"fixture-key","endpoint":proxy}}),
        );
        assert!(!invalid.has(&provider));
    }
}
