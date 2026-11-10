//! User-level connection profiles for the installed provider adapters.
use std::collections::BTreeMap;
use std::sync::LazyLock;

use pi_rs::{Error, Result};
use regex::{Captures, Regex};
use serde::Deserialize;
use serde_json::Value;

use super::{Provider, config};

#[derive(Clone, Default)]
pub(super) struct Profiles {
    entries: BTreeMap<String, Profile>,
}

#[derive(Clone)]
struct Profile {
    endpoint: String,
    api_key: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ConfiguredProfile {
    base_url: Option<String>,
    api: Option<String>,
    api_key: Option<String>,
}

impl Profiles {
    pub fn load() -> Result<Self> {
        let path = config::agent_dir().join("models.json");
        let data = config::read_object(&path)?;
        Self::parse(&data)
    }

    fn parse(data: &Value) -> Result<Self> {
        let Some(providers) = data.get("providers") else {
            return Ok(Self::default());
        };
        let providers = providers
            .as_object()
            .ok_or_else(|| Error::Config("models.json providers must be an object".into()))?;
        let mut entries = BTreeMap::new();
        for (name, entry) in providers {
            let Some(provider) = Provider::named(name) else {
                continue;
            };
            let profile = ConfiguredProfile::deserialize(entry).map_err(|_| {
                Error::Config(format!("invalid models.json profile for {}", provider.id()))
            })?;
            if profile
                .api
                .as_deref()
                .is_some_and(|api| api != provider.api())
            {
                return Err(Error::Config(format!(
                    "models.json profile {} requires the {} API",
                    provider.id(),
                    provider.api()
                )));
            }
            let endpoint = profile.base_url.as_deref().map_or_else(
                || Ok(provider.default_endpoint().to_owned()),
                |base| endpoint_from_base(provider, base),
            )?;
            entries.insert(
                provider.id().into(),
                Profile {
                    endpoint,
                    api_key: profile.api_key,
                },
            );
        }
        Ok(Self { entries })
    }

    pub fn endpoint(&self, provider: Provider) -> &str {
        self.entries
            .get(provider.id())
            .map_or_else(|| provider.default_endpoint(), |profile| &profile.endpoint)
    }

    pub fn api_key(&self, provider: Provider, endpoint: &str) -> Option<String> {
        let profile = self.entries.get(provider.id())?;
        if profile.endpoint != endpoint {
            return None;
        }
        expand_key(profile.api_key.as_deref()?)
    }
}

fn endpoint_from_base(provider: Provider, base: &str) -> Result<String> {
    let invalid = || Error::Config(format!("invalid models.json baseUrl for {}", provider.id()));
    let mut url = reqwest::Url::parse(base).map_err(|_| invalid())?;
    let loopback = matches!(url.host_str(), Some("localhost" | "127.0.0.1" | "[::1]"));
    if !(url.scheme() == "https" || (url.scheme() == "http" && loopback))
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(invalid());
    }

    let suffix = match provider {
        Provider::Ollama => "/chat/completions",
        Provider::Anthropic => "/messages",
        Provider::Google => {
            if url.path() == "/" {
                url.set_path("/v1beta");
            }
            return Ok(url.to_string());
        }
    };
    let path = url.path().trim_end_matches('/');
    let path = if path.ends_with(suffix) {
        path.to_owned()
    } else if path.is_empty() {
        format!("/v1{suffix}")
    } else {
        format!("{path}{suffix}")
    };
    url.set_path(&path);
    Ok(url.to_string())
}

fn expand_key(value: &str) -> Option<String> {
    if value.starts_with('!') {
        // Model profiles use literals or environment interpolation.
        return None;
    }
    static ENVIRONMENT: LazyLock<Regex> = LazyLock::new(|| {
        Regex::new(r"\$\$|\$!|\$\{([A-Za-z_][A-Za-z0-9_]*)\}|\$([A-Za-z_][A-Za-z0-9_]*)")
            .expect("environment interpolation")
    });
    let mut missing = false;
    let expanded = ENVIRONMENT.replace_all(value, |capture: &Captures<'_>| {
        let token = capture.get(0).expect("matched environment token").as_str();
        let Some(name) = capture.get(1).or_else(|| capture.get(2)) else {
            return token[1..].to_owned();
        };
        std::env::var(name.as_str()).unwrap_or_else(|_| {
            missing = true;
            String::new()
        })
    });
    (!missing && !expanded.trim().is_empty()).then(|| expanded.into_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    use serde_json::json;

    #[test]
    fn runtime_and_configured_keys_are_bound_to_their_connections() {
        let mut args = super::super::Cli::parse_from(["pi", "--api-key", "local-override"]);
        args.model_profiles = Profiles::parse(&json!({"providers":{
            "ollama":{"baseUrl":"http://127.0.0.1:7000/v1/","apiKey":"local-profile"},
            "anthropic":{"baseUrl":"http://127.0.0.1:7001/v1","apiKey":"anthropic-profile"}
        }}))
        .unwrap();
        args.endpoint = args.model_profiles.endpoint(Provider::Ollama).into();
        args.initial_connection = Some((Provider::Ollama, args.endpoint.clone()));
        args.model_scope
            .push(super::super::model_selection::Choice {
                model: "local-scoped".into(),
                thinking: None,
            });
        assert_eq!(args.endpoint, "http://127.0.0.1:7000/v1/chat/completions");
        assert_eq!(args.model_token().as_deref(), Some("local-override"));
        args.activate_provider(Provider::Anthropic).unwrap();
        assert!(args.model_scope.is_empty());
        assert_eq!(args.endpoint, "http://127.0.0.1:7001/v1/messages");
        assert_eq!(args.model_token().as_deref(), Some("anthropic-profile"));
        assert!(
            args.model_token_for(Provider::Anthropic, "http://127.0.0.1:7002/v1/messages")
                .is_none()
        );
        args.activate_provider(Provider::Ollama).unwrap();
        assert_eq!(args.model_scope[0].model, "local-scoped");
        assert_eq!(args.model_token().as_deref(), Some("local-override"));
    }
}
