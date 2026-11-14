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
    Invalid,
    ChatGpt,
    Kimi,
    Meta,
    BedrockProfile(String),
    BedrockChain,
}

type CommandResult = std::result::Result<String, &'static str>;
type CachedCommand = Arc<tokio::sync::OnceCell<CommandResult>>;
static STORED_COMMANDS: LazyLock<Mutex<BTreeMap<String, CachedCommand>>> =
    LazyLock::new(|| Mutex::new(BTreeMap::new()));

impl Credential {
    pub fn configured(value: &str) -> Option<Self> {
        Self::configured_with_env(value, None)
    }

    fn configured_with_env(
        value: &str,
        env: Option<&serde_json::Map<String, Value>>,
    ) -> Option<Self> {
        if let Some(command) = value.strip_prefix('!') {
            return (!command.trim().is_empty() && value.len() <= 16 * 1024)
                .then(|| Self::Command(value.into()));
        }
        super::model_profiles::expand_value_with_env(value, env)
            .filter(|value| !value.trim().is_empty())
            .map(Self::Literal)
    }

    pub fn literal(&self) -> Option<&str> {
        match self {
            Self::Literal(value) | Self::Bearer(value) => Some(value),
            Self::Command(_)
            | Self::StoredCommand(_)
            | Self::Invalid
            | Self::ChatGpt
            | Self::Kimi
            | Self::Meta
            | Self::BedrockProfile(_)
            | Self::BedrockChain => None,
        }
    }

    pub fn is_bearer(&self) -> bool {
        matches!(self, Self::Bearer(_) | Self::ChatGpt | Self::Kimi)
    }

    pub fn is_command(&self) -> bool {
        matches!(self, Self::Command(_) | Self::StoredCommand(_))
    }

    pub fn requires_resolution(&self) -> bool {
        self.is_command()
            || matches!(
                self,
                Self::ChatGpt
                    | Self::Kimi
                    | Self::Meta
                    | Self::Invalid
                    | Self::BedrockProfile(_)
                    | Self::BedrockChain
            )
    }

    pub async fn resolve(&self) -> Result<String> {
        let value = match self {
            Self::Literal(value) | Self::Bearer(value) => return Ok(value.clone()),
            Self::BedrockProfile(_) | Self::BedrockChain => {
                return Err(Error::Config(
                    "AWS credential sources cannot be used as an API key".into(),
                ));
            }
            Self::Invalid => return Err(Error::Config("Invalid model credential".into())),
            Self::ChatGpt => {
                let settings = super::config::Settings::load(&std::env::current_dir()?, false)?;
                return super::chatgpt_auth::access(&settings).await;
            }
            Self::Kimi => {
                let settings = super::config::Settings::load(&std::env::current_dir()?, false)?;
                return super::kimi_auth::access(&settings, None, false).await;
            }
            Self::Meta => {
                let settings = super::config::Settings::load(&std::env::current_dir()?, false)?;
                return super::meta_auth::access(&settings, None, false).await;
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
    key: Option<Credential>,
    env: Option<serde_json::Map<String, Value>>,
}

impl Keys {
    pub fn load() -> Result<Self> {
        Self::load_path(&config::agent_dir().join("auth.json"))
    }

    pub async fn load_async() -> Result<Self> {
        Self::load_path_async(&config::agent_dir().join("auth.json")).await
    }

    pub(super) async fn load_path_async(path: &std::path::Path) -> Result<Self> {
        let value = config::read_auth_object(path).await?;
        Ok(Self::parse(&value))
    }

    pub(super) fn load_path(path: &std::path::Path) -> Result<Self> {
        let value = config::read_object(path)?;
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
                let kimi = name.eq_ignore_ascii_case("kimi-coding") && entry["type"] == "oauth";
                let meta = name.eq_ignore_ascii_case("meta") && entry["type"] == "oauth";
                let oauth = (name.eq_ignore_ascii_case("openrouter") || chatgpt || kimi || meta)
                    && entry["type"] == "oauth";
                let key = if chatgpt {
                    serde_json::from_value::<pi_rs::openai::oauth::Credential>(
                        entry["credential"].clone(),
                    )
                    .ok()?;
                    Some(Credential::ChatGpt)
                } else if kimi {
                    let credential: super::kimi_auth::Credential =
                        serde_json::from_value(entry.clone()).ok()?;
                    if !valid_key(&credential.access) || !valid_key(&credential.refresh) {
                        return None;
                    }
                    Some(Credential::Kimi)
                } else if meta {
                    let valid =
                        serde_json::from_value::<super::meta_auth::Credential>(entry.clone())
                            .is_ok_and(|credential| {
                                valid_key(&credential.access) && valid_key(&credential.refresh)
                            });
                    Some(if valid {
                        Credential::Meta
                    } else {
                        Credential::Invalid
                    })
                } else if oauth {
                    let access = entry["access"].as_str()?;
                    Some(valid_key(access).then(|| Credential::Literal(access.into()))?)
                } else {
                    if entry["type"] != "api_key" {
                        return None;
                    }
                    if name.eq_ignore_ascii_case("amazon-bedrock") && entry.get("key").is_none() {
                        Some(match entry["env"].get("AWS_PROFILE") {
                            None => Credential::BedrockChain,
                            Some(profile) => match profile.as_str() {
                                Some(profile) if valid_profile(profile) => {
                                    Credential::BedrockProfile(profile.into())
                                }
                                _ => Credential::Invalid,
                            },
                        })
                    } else {
                        entry.get("key").map(|key| {
                            key.as_str().map_or(Credential::Invalid, |value| {
                                if entry["literal"] == true {
                                    return if valid_key(value) {
                                        Credential::Literal(value.into())
                                    } else {
                                        Credential::Invalid
                                    };
                                }
                                match Credential::configured_with_env(
                                    value,
                                    entry["env"].as_object(),
                                ) {
                                    Some(Credential::Command(command)) => {
                                        Credential::StoredCommand(command)
                                    }
                                    Some(Credential::Literal(value)) if valid_key(&value) => {
                                        Credential::Literal(value)
                                    }
                                    _ => Credential::Invalid,
                                }
                            })
                        })
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
                    && super::builtin_chat::find(if kimi {
                        "kimi-coding"
                    } else if meta {
                        "meta"
                    } else {
                        "openrouter"
                    })
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
                        env: entry["env"].as_object().cloned(),
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

    pub(super) fn forget(&mut self, provider: &str) {
        if self.entries.remove(provider).is_some() {
            self.revision = format!(
                "{:x}",
                Sha256::digest(format!("{}:{provider}", self.revision))
            );
        }
    }

    pub fn credential(&self, provider: &Provider, endpoint: &str) -> Option<&Credential> {
        self.entry_for(provider, endpoint)?.key.as_ref()
    }

    fn entry_for(&self, provider: &Provider, endpoint: &str) -> Option<&SavedKey> {
        let entry = self.entries.get(provider.id())?;
        let legacy_radius = provider == &Provider::Radius
            && endpoint == super::radius_auth::DEFAULT_MODEL_ENDPOINT
            && entry.endpoint.as_deref() == Some("https://radius.pi.dev/messages");
        let builtin_route = entry.official_routes
            && super::builtin_chat::find(provider.id())
                .is_some_and(|builtin| builtin.is_official_endpoint(endpoint));
        (entry.endpoint.as_deref() == Some(endpoint) || legacy_radius || builtin_route)
            .then_some(entry)
    }

    pub fn custom_credential(
        &self,
        provider: &Provider,
        endpoint: &str,
        profiles: &super::model_profiles::Profiles,
    ) -> Option<&Credential> {
        let entry = self.entries.get(provider.id())?;
        (entry.endpoint.is_none() && profiles.has_provider_endpoint(provider, endpoint))
            .then_some(entry.key.as_ref())
            .flatten()
    }

    pub fn environment(
        &self,
        provider: &Provider,
        endpoint: &str,
        profiles: &super::model_profiles::Profiles,
    ) -> Option<&serde_json::Map<String, Value>> {
        self.entry_for(provider, endpoint)
            .or_else(|| {
                self.entries.get(provider.id()).filter(|entry| {
                    entry.endpoint.is_none() && profiles.has_provider_endpoint(provider, endpoint)
                })
            })?
            .env
            .as_ref()
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
        self.operation_entry(provider, endpoint, official)?
            .key
            .as_ref()
    }

    pub fn operation_environment(
        &self,
        provider: &str,
        endpoint: &str,
        official: bool,
    ) -> Option<&serde_json::Map<String, Value>> {
        self.operation_entry(provider, endpoint, official)?
            .env
            .as_ref()
    }

    // Only unscoped, official-route credentials may supply the account used
    // to construct Cloudflare's official endpoint.
    pub(super) fn operation_official_environment(
        &self,
        provider: &str,
    ) -> Option<&serde_json::Map<String, Value>> {
        self.entries
            .get(provider)
            .filter(|entry| entry.official_routes)?
            .env
            .as_ref()
    }

    fn operation_entry(&self, provider: &str, endpoint: &str, official: bool) -> Option<&SavedKey> {
        let entry = self.entries.get(provider)?;
        // The provider-key UI binds a Vercel key to its official chat route.
        // Pi uses that same key for the gateway's separate classifier route.
        let vercel_classifier = provider == "vercel-ai-gateway"
            && official
            && entry.endpoint.as_deref() == Some("https://ai-gateway.vercel.sh/v1/messages")
            && endpoint == "https://ai-gateway.vercel.sh/typesafe/v1/systemone";
        (entry
            .endpoint
            .as_deref()
            .is_some_and(|saved| same_operation_endpoint(saved, endpoint))
            || vercel_classifier
            || (official
                && ((entry
                    .endpoint
                    .as_deref()
                    .zip(super::operation_models::official_base(provider))
                    .is_some_and(|(saved, base)| same_operation_endpoint(saved, base)))
                    || (entry.official_routes && super::builtin_chat::find(provider).is_some()))))
        .then_some(entry)
    }

    #[cfg(test)]
    fn get(&self, provider: &Provider, endpoint: &str) -> Option<&str> {
        self.credential(provider, endpoint)?.literal()
    }

    pub fn has(&self, provider: &Provider) -> bool {
        self.entries.contains_key(provider.id())
    }
}

fn same_operation_endpoint(left: &str, right: &str) -> bool {
    fn normalize(value: &str) -> Option<reqwest::Url> {
        let mut url = reqwest::Url::parse(value).ok()?;
        if !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return None;
        }
        let path = url.path().trim_end_matches('/').to_owned();
        url.set_path(&path);
        Some(url)
    }
    normalize(left)
        .zip(normalize(right))
        .is_some_and(|(a, b)| a == b)
}

pub(super) fn save(provider: &Provider, endpoint: &str, key: &str) -> Result<()> {
    save_at(
        &config::agent_dir().join("auth.json"),
        provider,
        endpoint,
        key,
    )
}

fn save_at(path: &std::path::Path, provider: &Provider, endpoint: &str, key: &str) -> Result<()> {
    let key = key.trim();
    if !valid_key(key) {
        return Err(Error::Config(
            "API key must contain 1–4096 printable ASCII characters without spaces".into(),
        ));
    }
    config::update_object(path, |data| {
        data.insert(
            provider.id().into(),
            json!({"type":"api_key","key":key,"endpoint":endpoint,"literal":true}),
        );
        Ok(())
    })
}

pub(super) async fn save_openrouter_oauth(key: &str) -> Result<()> {
    save_openrouter_oauth_at(&config::agent_dir().join("auth.json"), key).await
}

async fn save_openrouter_oauth_at(path: &std::path::Path, key: &str) -> Result<()> {
    let provider = Provider::named("openrouter").expect("built-in OpenRouter provider");
    config::update_auth_object(path, |data| {
        data.insert(
            provider.id().into(),
            json!({
                "type":"oauth","access":key,"refresh":"","expires":9_007_199_254_740_991_u64,
                "endpoint":provider.default_endpoint(),
            }),
        );
        Ok(())
    })
    .await
}

pub(super) fn valid_profile(profile: &str) -> bool {
    !profile.trim().is_empty() && profile.len() <= 1024 && !profile.chars().any(char::is_control)
}

pub(super) fn save_bedrock_profile(profile: Option<&str>) -> Result<()> {
    if profile.is_some_and(|profile| !valid_profile(profile)) {
        return Err(Error::Config("Enter a valid AWS profile name".into()));
    }
    save_bedrock_profile_at(&config::agent_dir().join("auth.json"), profile)
}

fn save_bedrock_profile_at(path: &std::path::Path, profile: Option<&str>) -> Result<()> {
    if profile.is_some_and(|profile| !valid_profile(profile)) {
        return Err(Error::Config("Enter a valid AWS profile name".into()));
    }
    config::update_object(path, |data| {
        let value = match profile {
            Some(profile) => json!({"type":"api_key", "env":{"AWS_PROFILE":profile}}),
            None => json!({"type":"api_key"}),
        };
        data.insert("amazon-bedrock".into(), value);
        Ok(())
    })
}

pub(super) fn remove(provider: &Provider) -> Result<bool> {
    let mut removed = false;
    config::update_object(&config::agent_dir().join("auth.json"), |data| {
        let kind = data
            .get(provider.id())
            .and_then(|value| value["type"].as_str());
        if kind == Some("api_key")
            || (matches!(provider.id(), "openrouter" | "kimi-coding" | "meta")
                && kind == Some("oauth"))
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
    #[test]
    fn vercel_ui_saved_key_only_bridges_exact_official_classifier_route() {
        use super::*;
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("auth.json");
        let provider = Provider::named("vercel-ai-gateway").unwrap();
        let chat = "https://ai-gateway.vercel.sh/v1/messages";
        let classifier = "https://ai-gateway.vercel.sh/typesafe/v1/systemone";
        assert_eq!(provider.default_endpoint(), chat);
        save_at(&path, &provider, chat, "ui-fixture-key").unwrap();
        let stored = config::read_object(&path).unwrap();
        assert_eq!(stored[provider.id()]["endpoint"], chat);
        assert_eq!(stored[provider.id()]["literal"], true);
        let keys = Keys::load_path(&path).unwrap();
        assert_eq!(
            keys.operation_credential(provider.id(), classifier, true)
                .unwrap()
                .literal(),
            Some("ui-fixture-key")
        );
        assert!(
            keys.operation_credential(provider.id(), classifier, false)
                .is_none()
        );
        for destination in [
            "https://example.test/typesafe/v1/systemone",
            "https://ai-gateway.vercel.sh:444/typesafe/v1/systemone",
            "https://ai-gateway.vercel.sh/other/systemone",
        ] {
            assert!(
                keys.operation_credential(provider.id(), destination, true)
                    .is_none()
            );
        }
        save_at(
            &path,
            &provider,
            "https://example.test/v1/messages",
            "custom-key",
        )
        .unwrap();
        let keys = Keys::load_path(&path).unwrap();
        assert!(
            keys.operation_credential(provider.id(), classifier, true)
                .is_none()
        );
    }
    #[test]
    fn bedrock_profile_and_chain_restore_in_pi_format() {
        use super::*;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("auth.json");
        std::fs::write(&path, r#"{"other":{"type":"api_key","key":"keep"}}"#).unwrap();
        save_bedrock_profile_at(&path, Some("work profile")).unwrap();
        let value = config::read_object(&path).unwrap();
        assert_eq!(
            value["amazon-bedrock"],
            json!({"type":"api_key","env":{"AWS_PROFILE":"work profile"}})
        );
        assert_eq!(value["other"]["key"], "keep");
        let keys = Keys::load_path(&path).unwrap();
        assert!(
            matches!(keys.credential(&Provider::Bedrock, ""), Some(Credential::BedrockProfile(profile)) if profile == "work profile")
        );
        assert!(save_bedrock_profile_at(&path, Some("bad\nprofile")).is_err());
        save_bedrock_profile_at(&path, None).unwrap();
        let keys = Keys::load_path(&path).unwrap();
        assert!(keys.has(&Provider::Bedrock));
        assert!(matches!(
            keys.credential(&Provider::Bedrock, ""),
            Some(Credential::BedrockChain)
        ));
        assert_eq!(
            config::read_object(&path).unwrap()["amazon-bedrock"],
            json!({"type":"api_key"})
        );
    }
    use super::*;

    #[tokio::test]
    async fn async_key_read_and_openrouter_save_wait_for_shared_lock() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("auth.json");
        std::fs::write(&path, r#"{"other":{"type":"api_key","key":"keep"}}"#).unwrap();
        let held = super::super::auth_lock::acquire_async(&path).await.unwrap();
        let mut writer = tokio::spawn({
            let path = path.clone();
            async move { save_openrouter_oauth_at(&path, "new-key").await }
        });
        let mut reader = tokio::spawn({
            let path = path.clone();
            async move { Keys::load_path_async(&path).await }
        });
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(120), &mut writer)
                .await
                .is_err()
        );
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(120), &mut reader)
                .await
                .is_err()
        );
        drop(held);
        writer.await.unwrap().unwrap();
        reader.await.unwrap().unwrap();
        let stored = super::super::config::read_auth_object(&path).await.unwrap();
        assert_eq!(stored["openrouter"]["access"], "new-key");
        assert_eq!(stored["other"]["key"], "keep");
    }

    #[test]
    fn logout_forgets_key_before_a_reload_can_be_cancelled() {
        let endpoint = Provider::named("openrouter").unwrap().default_endpoint();
        let mut keys = Keys::parse(&json!({
            "openrouter": {"type":"oauth", "access":"old", "endpoint":endpoint},
            "other": {"type":"api_key", "key":"keep", "endpoint":"https://example.test"}
        }));
        assert!(
            keys.operation_credential("openrouter", endpoint, true)
                .is_some()
        );
        let old_revision = keys.revision().to_owned();
        keys.forget("openrouter");
        assert_ne!(keys.revision(), old_revision);
        assert!(
            keys.operation_credential("openrouter", endpoint, true)
                .is_none()
        );
        assert!(
            keys.operation_credential("other", "https://example.test", false)
                .is_some()
        );
    }

    #[test]
    fn saved_operation_endpoint_ignores_trailing_slashes_only() {
        for (saved, requested) in [
            (
                "https://proxy.example/v1/systemone/",
                "https://proxy.example/v1/systemone",
            ),
            (
                "https://proxy.example/v1/systemone",
                "https://proxy.example/v1/systemone/",
            ),
        ] {
            let keys = Keys::parse(&json!({"proxy":{
                "type":"api_key", "key":"stored-key", "endpoint":saved,
                "env":{"HEADER":"secret"}
            }}));
            assert_eq!(
                keys.operation_credential("proxy", requested, false)
                    .and_then(Credential::literal),
                Some("stored-key")
            );
            assert_eq!(
                keys.operation_environment("proxy", requested, false)
                    .unwrap()["HEADER"],
                "secret"
            );
            for other in [
                "https://other.example/v1/systemone",
                "https://proxy.example:8443/v1/systemone",
                "http://proxy.example/v1/systemone",
                "https://proxy.example/v1/other",
                "https://proxy.example/v1/systemone?x=1",
                "https://proxy.example/v1/systemone#fragment",
                "https://user@proxy.example/v1/systemone",
            ] {
                assert!(keys.operation_credential("proxy", other, false).is_none());
                assert!(keys.operation_environment("proxy", other, false).is_none());
            }
        }
    }

    #[tokio::test]
    async fn stored_api_key_interpolates_credential_environment() {
        let keys = Keys::parse(&json!({
            "anthropic": {"type":"api_key", "key":"prefix-${TOKEN}-$$", "env":{"TOKEN":"credential-secret"}},
            "typesafe": {"type":"api_key", "key":"$TOKEN", "env":{"TOKEN":"operation-secret"}},
            "openai": {"type":"api_key", "key":"$TOKEN", "literal":true, "env":{"TOKEN":"ignored"}}
        }));
        let key = keys
            .credential(&Provider::Anthropic, Provider::Anthropic.default_endpoint())
            .unwrap();
        assert_eq!(key.resolve().await.unwrap(), "prefix-credential-secret-$");
        let key = keys
            .operation_credential("typesafe", "https://api.typesafe.ai/v1/systemone", true)
            .unwrap();
        assert_eq!(key.resolve().await.unwrap(), "operation-secret");
        let openai = Provider::named("openai").unwrap();
        assert_eq!(
            keys.credential(&openai, openai.default_endpoint())
                .unwrap()
                .resolve()
                .await
                .unwrap(),
            "$TOKEN"
        );
        let local = json!({"PATH":"credential-path"});
        assert_eq!(
            super::super::model_profiles::expand_value_with_env("$PATH", local.as_object())
                .as_deref(),
            Some("credential-path")
        );
        let empty = json!({"PATH":""});
        assert_eq!(
            super::super::model_profiles::expand_value_with_env("$PATH", empty.as_object()),
            std::env::var("PATH").ok().filter(|v| !v.is_empty())
        );
    }

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
    fn kimi_pi_oauth_is_endpoint_bound_and_bearer() {
        let provider = Provider::named("kimi-coding").unwrap();
        let keys = Keys::parse(
            &json!({"kimi-coding":{"type":"oauth","access":"access","refresh":"refresh","expires":123}}),
        );
        let credential = keys
            .credential(&provider, provider.default_endpoint())
            .unwrap();
        assert!(matches!(credential, Credential::Kimi));
        assert!(credential.is_bearer());
        assert!(keys.oauth(&provider, provider.default_endpoint()));
        assert!(
            keys.credential(&provider, "http://localhost:1234/v1/messages")
                .is_none()
        );
        let invalid = Keys::parse(
            &json!({"kimi-coding":{"type":"oauth","access":"access","refresh":"refresh","expires":123,"endpoint":"http://localhost:1234/v1/messages"}}),
        );
        assert!(
            invalid
                .credential(&provider, provider.default_endpoint())
                .is_none()
        );
    }

    #[test]
    fn meta_pi_oauth_is_endpoint_bound_and_malformed_entries_fail_closed() {
        let provider = Provider::named("meta").unwrap();
        let official = provider.default_endpoint();
        let keys = Keys::parse(
            &json!({"meta":{"type":"oauth","access":"minted","refresh":"identity","expires":123}}),
        );
        let credential = keys.credential(&provider, official).unwrap();
        assert!(matches!(credential, Credential::Meta));
        assert!(!credential.is_bearer());
        assert!(credential.requires_resolution());
        assert!(keys.oauth(&provider, official));
        assert!(
            keys.credential(&provider, "https://attacker.example/v1/responses")
                .is_none()
        );
        assert!(
            keys.credential(&provider, "https://api.meta.ai:444/v1/responses")
                .is_none()
        );
        let malformed =
            Keys::parse(&json!({"meta":{"type":"oauth","access":"minted","expires":123}}));
        assert!(matches!(
            malformed.credential(&provider, official),
            Some(Credential::Invalid)
        ));
        let displaced = Keys::parse(
            &json!({"meta":{"type":"oauth","access":"minted","refresh":"identity","expires":123,"endpoint":"https://attacker.example/v1/responses"}}),
        );
        assert!(displaced.credential(&provider, official).is_none());
        assert!(
            displaced
                .credential(&provider, "https://attacker.example/v1/responses")
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
