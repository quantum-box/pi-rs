//! Private API keys for native model providers.
use std::collections::BTreeMap;

use pi_rs::{Error, Result};
use serde_json::{Value, json};

use super::{Provider, config};

#[derive(Clone, Default)]
pub(super) struct Keys {
    entries: BTreeMap<String, SavedKey>,
}

#[derive(Clone)]
struct SavedKey {
    endpoint: String,
    key: String,
}

impl Keys {
    pub fn load() -> Result<Self> {
        let value = config::read_object(&config::agent_dir().join("auth.json"))?;
        Ok(Self::parse(&value))
    }

    fn parse(value: &Value) -> Self {
        let entries = Provider::ALL
            .into_iter()
            .filter_map(|provider| {
                let entry = value.get(provider.id())?;
                if entry["type"] != "api_key" {
                    return None;
                }
                let key = entry["key"].as_str().filter(|key| valid_key(key))?;
                let endpoint = match entry.get("endpoint") {
                    None => provider.default_endpoint(),
                    Some(value) => value.as_str()?,
                };
                Some((
                    provider.id().to_owned(),
                    SavedKey {
                        endpoint: endpoint.to_owned(),
                        key: key.to_owned(),
                    },
                ))
            })
            .collect();
        Self { entries }
    }

    pub fn get(&self, provider: Provider, endpoint: &str) -> Option<&str> {
        let entry = self.entries.get(provider.id())?;
        (entry.endpoint == endpoint).then_some(entry.key.as_str())
    }

    pub fn has(&self, provider: Provider) -> bool {
        self.entries.contains_key(provider.id())
    }
}

pub(super) fn save(provider: Provider, endpoint: &str, key: &str) -> Result<()> {
    let key = key.trim();
    if !valid_key(key) {
        return Err(Error::Config(
            "API key must contain 1–4096 printable ASCII characters without spaces".into(),
        ));
    }
    config::update_object(&config::agent_dir().join("auth.json"), |data| {
        data.insert(
            provider.id().into(),
            json!({"type":"api_key","key":key,"endpoint":endpoint}),
        );
        Ok(())
    })
}

pub(super) fn remove(provider: Provider) -> Result<bool> {
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
        assert_eq!(keys.get(Provider::Ollama, proxy), Some("proxy-key"));
        assert!(
            keys.get(Provider::Ollama, Provider::Ollama.default_endpoint())
                .is_none()
        );
        assert_eq!(
            keys.get(Provider::Anthropic, Provider::Anthropic.default_endpoint()),
            Some("legacy-key")
        );
        assert!(
            keys.get(Provider::Anthropic, "http://127.0.0.1:8001/v1/messages")
                .is_none()
        );
    }
}
