//! Private API keys for native model providers.
use std::collections::BTreeMap;

use pi_rs::{Error, Result};
use serde_json::{Value, json};

use super::{Provider, config};

#[derive(Clone)]
pub(super) enum Credential {
    Literal(String),
    Command(String),
}

impl Credential {
    pub fn configured(value: &str) -> Option<Self> {
        if let Some(command) = value.strip_prefix('!') {
            return (!command.trim().is_empty() && value.len() <= 16 * 1024)
                .then(|| Self::Command(value.into()));
        }
        super::model_profiles::expand_value(value)
            .filter(|value| valid_key(value))
            .map(Self::Literal)
    }

    pub fn literal(&self) -> Option<&str> {
        match self {
            Self::Literal(value) => Some(value),
            Self::Command(_) => None,
        }
    }

    pub async fn resolve(&self) -> Result<String> {
        match self {
            Self::Literal(value) => Ok(value.clone()),
            Self::Command(command) => {
                let value = pi_rs_workbench::mcp::resolve_value(command)
                    .await
                    .map_err(|_| Error::Config("Model credential command failed".into()))?;
                if !valid_key(&value) {
                    return Err(Error::Config(
                        "Model credential command returned an invalid key".into(),
                    ));
                }
                Ok(value)
            }
        }
    }
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
                    Credential::configured(value)?
                };
                let endpoint = match entry.get("endpoint") {
                    None => Provider::named(name)?.default_endpoint(),
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
        (entry.endpoint == endpoint).then_some(&entry.key)
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

fn valid_key(key: &str) -> bool {
    !key.is_empty() && key.len() <= 4096 && key.bytes().all(|byte| byte.is_ascii_graphic())
}

#[cfg(test)]
mod tests {
    use super::*;

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
