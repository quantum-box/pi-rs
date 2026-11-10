//! User-level connection profiles for the installed provider adapters.
use std::collections::BTreeMap;
use std::sync::{Arc, LazyLock};

use pi_rs::{Error, Result};
use regex::{Captures, Regex};
use reqwest::header::{AUTHORIZATION, HeaderMap, HeaderName, HeaderValue};
use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest, Sha256};

use super::{Provider, config, providers::Adapter};

#[derive(Clone, Default)]
pub(super) struct Profiles {
    entries: BTreeMap<String, Profile>,
    revision: String,
}

#[derive(Clone)]
struct Profile {
    provider: Provider,
    endpoint: String,
    api_key: Option<String>,
    headers: BTreeMap<String, String>,
    auth_header: bool,
    models: Vec<ConfiguredModel>,
    overrides: BTreeMap<String, ModelMetadata>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ConfiguredProfile {
    base_url: Option<String>,
    api: Option<String>,
    api_key: Option<String>,
    #[serde(default)]
    headers: BTreeMap<String, String>,
    #[serde(default)]
    auth_header: bool,
    #[serde(default)]
    models: Vec<ConfiguredModel>,
    #[serde(default)]
    model_overrides: BTreeMap<String, ModelMetadata>,
}

#[derive(Clone, Deserialize)]
struct ConfiguredModel {
    id: String,
    #[serde(flatten)]
    metadata: ModelMetadata,
}

#[derive(Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct ModelMetadata {
    pub name: Option<String>,
    pub context_window: Option<u32>,
    pub max_tokens: Option<u32>,
    pub reasoning: Option<bool>,
    input: Option<Vec<Modality>>,
    #[serde(default)]
    thinking_level_map: BTreeMap<String, Option<String>>,
    #[serde(default)]
    sampling_params: BTreeMap<String, Value>,
    #[serde(default)]
    sampling_params_by_thinking_level: BTreeMap<String, BTreeMap<String, Value>>,
    #[serde(default)]
    headers: BTreeMap<String, String>,
}

#[derive(Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
enum Modality {
    Text,
    Image,
}

impl ModelMetadata {
    fn with_model_defaults(mut self) -> Self {
        self.context_window.get_or_insert(128_000);
        self.max_tokens.get_or_insert(16_384);
        self.reasoning.get_or_insert(false);
        self.input.get_or_insert_with(|| vec![Modality::Text]);
        self
    }

    fn overlay(mut self, overrides: &Self) -> Self {
        self.thinking_level_map
            .extend(overrides.thinking_level_map.clone());
        self.sampling_params
            .extend(overrides.sampling_params.clone());
        self.headers.extend(overrides.headers.clone());
        for (level, parameters) in &overrides.sampling_params_by_thinking_level {
            self.sampling_params_by_thinking_level
                .entry(level.clone())
                .or_default()
                .extend(parameters.clone());
        }
        Self {
            name: overrides.name.clone().or(self.name),
            context_window: overrides.context_window.or(self.context_window),
            max_tokens: overrides.max_tokens.or(self.max_tokens),
            reasoning: overrides.reasoning.or(self.reasoning),
            input: overrides.input.clone().or(self.input),
            ..self
        }
    }

    fn validate(&self, provider: &Provider, id: &str) -> Result<()> {
        if id.trim().is_empty()
            || self
                .name
                .as_ref()
                .is_some_and(|name| name.trim().is_empty())
            || self.context_window == Some(0)
            || self.max_tokens == Some(0)
        {
            return Err(Error::Config(format!(
                "invalid model metadata for {}/{id}",
                provider.id()
            )));
        }
        if provider.adapter() == Adapter::ChatCompletions {
            pi_rs::ollama::validate_sampling(&self.sampling_params)?;
            for (level, parameters) in &self.sampling_params_by_thinking_level {
                if !super::thinking::LEVELS.contains(&level.as_str()) {
                    return Err(Error::Config(format!(
                        "unknown sampling thinking level {level:?}"
                    )));
                }
                pi_rs::ollama::validate_sampling(parameters)?;
            }
            for (level, value) in &self.thinking_level_map {
                if !super::thinking::LEVELS.contains(&level.as_str())
                    || value.as_ref().is_some_and(|value| value.trim().is_empty())
                {
                    return Err(Error::Config(format!(
                        "invalid mapping for thinking level {level:?}"
                    )));
                }
            }
        }
        Ok(())
    }

    pub fn levels(&self, provider: &Provider) -> Option<Vec<String>> {
        if self.reasoning == Some(false) {
            return Some(vec!["off".into()]);
        }
        if provider.adapter() != Adapter::ChatCompletions || self.thinking_level_map.is_empty() {
            return None;
        }
        Some(
            super::thinking::LEVELS
                .iter()
                .copied()
                .filter(|level| {
                    *level == "off"
                        || self
                            .thinking_level_map
                            .get(*level)
                            .map_or(!matches!(*level, "xhigh" | "max"), Option::is_some)
                })
                .map(str::to_owned)
                .collect(),
        )
    }

    pub fn mapped_effort(&self, level: &str) -> Option<Option<&str>> {
        self.thinking_level_map.get(level).map(Option::as_deref)
    }

    pub fn sampling(&self, level: &str) -> BTreeMap<String, Value> {
        let mut sampling = self.sampling_params.clone();
        if let Some(overrides) = self.sampling_params_by_thinking_level.get(level) {
            sampling.extend(overrides.clone());
        }
        sampling
    }

    pub fn apply(&self, provider: &Provider, details: &mut pi_rs::ollama::ModelDetails) {
        details.context_window = self.context_window.or(details.context_window);
        details.max_output_tokens = self.max_tokens.or(details.max_output_tokens);
        details.reasoning = self.reasoning.or(details.reasoning);
        if let Some(input) = &self.input {
            details.images = Some(input.contains(&Modality::Image));
        }
        if let Some(levels) = self.levels(provider) {
            details.thinking_levels = Some(levels);
        }
    }
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
            let provider = if let Some(provider) = Provider::named(name) {
                provider
            } else {
                let Some(adapter) = entry["api"].as_str().and_then(Adapter::named) else {
                    continue;
                };
                if name.is_empty()
                    || !name.bytes().all(|byte| {
                        byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.')
                    })
                {
                    return Err(Error::Config("invalid custom model provider name".into()));
                }
                if entry["baseUrl"].as_str().is_none() {
                    return Err(Error::Config(format!(
                        "custom model provider {name} requires baseUrl"
                    )));
                }
                Provider::Custom {
                    id: Arc::from(name.to_ascii_lowercase()),
                    adapter,
                }
            };
            let mut profile = ConfiguredProfile::deserialize(entry).map_err(|_| {
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
                |base| endpoint_from_base(&provider, base),
            )?;
            let mut ids = std::collections::BTreeSet::new();
            for model in &mut profile.models {
                model.metadata.validate(&provider, &model.id)?;
                model.metadata.headers =
                    validate_headers(std::mem::take(&mut model.metadata.headers))?;
                if !ids.insert(model.id.as_str()) {
                    return Err(Error::Config(format!(
                        "duplicate model {}/{}",
                        provider.id(),
                        model.id
                    )));
                }
            }
            for (id, metadata) in &mut profile.model_overrides {
                metadata.validate(&provider, id)?;
                metadata.headers = validate_headers(std::mem::take(&mut metadata.headers))?;
            }
            let id = provider.id().to_owned();
            if entries
                .insert(
                    id.clone(),
                    Profile {
                        provider,
                        endpoint,
                        api_key: profile.api_key,
                        headers: validate_headers(profile.headers)?,
                        auth_header: profile.auth_header,
                        models: profile.models,
                        overrides: profile.model_overrides,
                    },
                )
                .is_some()
            {
                return Err(Error::Config(format!("duplicate model provider {id}")));
            }
        }
        let revision = format!("{:x}", Sha256::digest(serde_json::to_vec(data)?));
        Ok(Self { entries, revision })
    }

    pub fn revision(&self) -> &str {
        &self.revision
    }

    pub fn provider(&self, name: &str) -> Option<Provider> {
        Provider::named(name).or_else(|| {
            self.entries
                .get(&name.to_ascii_lowercase())
                .map(|profile| profile.provider.clone())
        })
    }

    pub fn providers(&self) -> impl Iterator<Item = Provider> + '_ {
        Provider::ALL.into_iter().chain(
            self.entries
                .values()
                .filter(|profile| matches!(profile.provider, Provider::Custom { .. }))
                .map(|profile| profile.provider.clone()),
        )
    }

    pub fn endpoint(&self, provider: &Provider) -> &str {
        self.entries
            .get(provider.id())
            .map_or_else(|| provider.default_endpoint(), |profile| &profile.endpoint)
    }

    pub fn api_key(&self, provider: &Provider, endpoint: &str) -> Option<String> {
        let profile = self.entries.get(provider.id())?;
        if profile.endpoint != endpoint {
            return None;
        }
        expand_key(profile.api_key.as_deref()?)
    }

    pub fn request_headers(
        &self,
        provider: &Provider,
        endpoint: &str,
        model: Option<&str>,
        token: Option<&str>,
    ) -> Result<HeaderMap> {
        let Some(profile) = self
            .entries
            .get(provider.id())
            .filter(|profile| profile.endpoint == endpoint)
        else {
            return Ok(HeaderMap::new());
        };
        let mut configured = profile.headers.clone();
        if let Some(metadata) = model.and_then(|id| self.model(provider, id)) {
            configured.extend(metadata.headers);
        }
        let mut headers =
            HeaderMap::with_capacity(configured.len() + usize::from(profile.auth_header));
        for (name, value) in configured {
            let value = expand_value(&value)
                .ok_or_else(|| Error::Config(format!("could not resolve model header {name}")))?;
            headers.insert(
                HeaderName::from_bytes(name.as_bytes()).expect("validated header name"),
                sensitive_header(&name, &value)?,
            );
        }
        if profile.auth_header {
            let token = token
                .ok_or_else(|| Error::Config("authHeader requires a resolved API key".into()))?;
            headers.insert(
                AUTHORIZATION,
                sensitive_header("authorization", &format!("Bearer {token}"))?,
            );
        }
        Ok(headers)
    }

    pub fn model_ids(&self, provider: &Provider) -> Vec<String> {
        self.entries
            .get(provider.id())
            .into_iter()
            .flat_map(|profile| profile.models.iter().map(|model| model.id.clone()))
            .collect()
    }

    pub fn model(&self, provider: &Provider, id: &str) -> Option<ModelMetadata> {
        let profile = self.entries.get(provider.id())?;
        let model = profile
            .models
            .iter()
            .find(|model| model.id == id)
            .map(|model| model.metadata.clone().with_model_defaults());
        match profile.overrides.get(id) {
            Some(overrides) => Some(model.unwrap_or_default().overlay(overrides)),
            None => model,
        }
    }
}

fn endpoint_from_base(provider: &Provider, base: &str) -> Result<String> {
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

    let suffix = match provider.adapter() {
        Adapter::ChatCompletions => "/chat/completions",
        Adapter::Anthropic | Adapter::PiMessages => "/messages",
        Adapter::Google => {
            if url.path() == "/" {
                url.set_path("/v1beta");
            }
            return Ok(url.to_string());
        }
    };
    let path = url.path().trim_end_matches('/');
    let path = if path.ends_with(suffix) {
        path.to_owned()
    } else if path.is_empty() && provider.adapter() != Adapter::PiMessages {
        format!("/v1{suffix}")
    } else {
        format!("{path}{suffix}")
    };
    url.set_path(&path);
    Ok(url.to_string())
}

fn expand_key(value: &str) -> Option<String> {
    expand_value(value).filter(|value| !value.trim().is_empty())
}

fn expand_value(value: &str) -> Option<String> {
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
    (!missing).then(|| expanded.into_owned())
}

fn validate_headers(headers: BTreeMap<String, String>) -> Result<BTreeMap<String, String>> {
    let mut result = BTreeMap::new();
    for (key, value) in headers {
        let key = HeaderName::from_bytes(key.as_bytes())
            .map_err(|_| Error::Config("invalid model header name".into()))?;
        if matches!(
            key.as_str(),
            "host"
                | "content-length"
                | "transfer-encoding"
                | "connection"
                | "proxy-authorization"
                | "proxy-connection"
                | "te"
                | "upgrade"
                | "trailer"
        ) {
            return Err(Error::Config(format!(
                "model header {key} is managed by the HTTP transport"
            )));
        }
        if result.insert(key.as_str().to_owned(), value).is_some() {
            return Err(Error::Config(format!("duplicate model header {key}")));
        }
    }
    Ok(result)
}

fn sensitive_header(name: &str, value: &str) -> Result<HeaderValue> {
    let mut value = HeaderValue::from_str(value)
        .map_err(|_| Error::Config(format!("invalid value for model header {name}")))?;
    value.set_sensitive(true);
    Ok(value)
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
        args.endpoint = args.model_profiles.endpoint(&Provider::Ollama).into();
        args.initial_connection = Some((Provider::Ollama, args.endpoint.clone()));
        args.model_scope
            .push(super::super::model_selection::ScopedChoice {
                provider: Provider::Ollama,
                model: "local-scoped".into(),
                thinking: None,
            });
        assert_eq!(args.endpoint, "http://127.0.0.1:7000/v1/chat/completions");
        assert_eq!(args.model_token().as_deref(), Some("local-override"));
        args.activate_provider(Provider::Anthropic).unwrap();
        assert_eq!(args.model_scope[0].provider.id(), "ollama");
        assert_eq!(args.endpoint, "http://127.0.0.1:7001/v1/messages");
        assert_eq!(args.model_token().as_deref(), Some("anthropic-profile"));
        assert!(
            args.model_token_for(&Provider::Anthropic, "http://127.0.0.1:7002/v1/messages")
                .is_none()
        );
        args.activate_provider(Provider::Ollama).unwrap();
        assert_eq!(args.model_scope[0].model, "local-scoped");
        assert_eq!(args.model_token().as_deref(), Some("local-override"));
        args.model = "local-scoped".into();
        args.restore_model_selection("outside-scope".into(), None, Some("anthropic"));
        assert_eq!(args.provider.id(), "ollama");
        assert_eq!(args.model, "local-scoped");
    }

    #[test]
    fn declared_models_and_overrides_preserve_identity_and_unset_fields() {
        let profiles = Profiles::parse(&json!({"providers":{"ollama":{
            "models":[{"id":"custom","name":"Local Fixture","input":["text","image"],"maxTokens":200}],
            "modelOverrides":{
                "custom":{"maxTokens":100},
                "existing":{"contextWindow":32000}
            }
        }}})).unwrap();
        assert_eq!(profiles.model_ids(&Provider::Ollama), ["custom"]);
        let custom = profiles.model(&Provider::Ollama, "custom").unwrap();
        assert_eq!(custom.name.as_deref(), Some("Local Fixture"));
        assert_eq!(custom.max_tokens, Some(100));
        assert_eq!(custom.context_window, Some(128_000));
        let mut details = pi_rs::ollama::ModelDetails::default();
        custom.apply(&Provider::Ollama, &mut details);
        assert_eq!(details.images, Some(true));
        assert_eq!(details.thinking_levels, Some(vec!["off".into()]));
        let existing = profiles.model(&Provider::Ollama, "existing").unwrap();
        assert_eq!(existing.context_window, Some(32000));
        assert_eq!(existing.max_tokens, None);
    }

    #[test]
    fn sampling_layers_merge_without_overriding_agent_fields() {
        let profiles = Profiles::parse(&json!({"providers":{"ollama":{
            "models":[{"id":"custom","reasoning":true,
                "thinkingLevelMap":{"medium":null,"xhigh":"strong"},
                "samplingParams":{"temperature":0.7,"top_p":0.9},
                "samplingParamsByThinkingLevel":{"xhigh":{"temperature":0.5,"top_k":20}}
            }],
            "modelOverrides":{"custom":{"samplingParamsByThinkingLevel":{"xhigh":{"temperature":0.2}}}}
        }}})).unwrap();
        let model = profiles.model(&Provider::Ollama, "custom").unwrap();
        let levels = model.levels(&Provider::Ollama).unwrap();
        assert!(
            !levels
                .iter()
                .any(|level| level == "medium" || level == "max")
        );
        assert_eq!(model.mapped_effort("xhigh"), Some(Some("strong")));
        assert_eq!(
            model.sampling("xhigh"),
            BTreeMap::from([
                ("temperature".into(), json!(0.2)),
                ("top_k".into(), json!(20)),
                ("top_p".into(), json!(0.9)),
            ])
        );
        assert!(
            Profiles::parse(&json!({"providers":{"ollama":{
                "models":[{"id":"invalid","samplingParams":{"messages":[]}}]
            }}}))
            .is_err()
        );
    }

    #[test]
    fn named_profiles_keep_their_adapter_endpoint_and_model_identity() {
        let profiles = Profiles::parse(&json!({"providers":{
            "Local-Relay":{"api":"openai-completions","baseUrl":"http://127.0.0.1:7000/v1","models":[{"id":"same"}]},
            "message-proxy":{"api":"anthropic-messages","baseUrl":"http://127.0.0.1:7001/v1","models":[{"id":"same"}]}
        }})).unwrap();
        let relay = profiles.provider("LOCAL-RELAY").unwrap();
        let proxy = profiles.provider("message-proxy").unwrap();
        assert_eq!(relay.id(), "local-relay");
        assert_eq!(relay.adapter(), Adapter::ChatCompletions);
        assert_eq!(proxy.adapter(), Adapter::Anthropic);
        assert_ne!(relay, proxy);
        assert_eq!(profiles.model_ids(&relay), ["same"]);
        assert_eq!(
            profiles.endpoint(&proxy),
            "http://127.0.0.1:7001/v1/messages"
        );
    }

    #[test]
    fn request_headers_merge_case_insensitively_and_stay_endpoint_bound() {
        let profiles = Profiles::parse(&json!({"providers":{"ollama":{
            "baseUrl":"http://127.0.0.1:7000/v1",
            "headers":{"X-Route":"base","X-Extra":"$$literal"},
            "authHeader":true,
            "models":[{"id":"custom","headers":{"x-route":"model"}}],
            "modelOverrides":{"custom":{"headers":{"X-ROUTE":"override"}}}
        }}}))
        .unwrap();
        let provider = Provider::Ollama;
        let endpoint = profiles.endpoint(&provider);
        let headers = profiles
            .request_headers(&provider, endpoint, Some("custom"), Some("secret"))
            .unwrap();
        assert_eq!(headers["x-route"], "override");
        assert_eq!(headers["x-extra"], "$literal");
        assert_eq!(headers[AUTHORIZATION], "Bearer secret");
        assert!(headers[AUTHORIZATION].is_sensitive());
        assert!(
            profiles
                .request_headers(&provider, endpoint, None, None)
                .is_err()
        );
        assert!(
            profiles
                .request_headers(
                    &provider,
                    "http://127.0.0.1:7001/v1/chat/completions",
                    Some("custom"),
                    Some("secret")
                )
                .unwrap()
                .is_empty()
        );
        assert!(
            Profiles::parse(&json!({"providers":{"ollama":{"headers":{"Host":"other"}}}})).is_err()
        );
    }
}
