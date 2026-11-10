//! User-level connection profiles for the installed provider adapters.
use std::collections::BTreeMap;
use std::sync::{Arc, LazyLock};

use pi_rs::ollama::ChatCompatibility;
use pi_rs::{Error, Result};
use regex::{Captures, Regex};
use reqwest::header::{AUTHORIZATION, HeaderMap, HeaderName, HeaderValue};
use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest, Sha256};

use super::{Provider, builtin_chat, config, model_auth::Credential, providers::Adapter};

#[derive(Clone, Default)]
pub(super) struct Profiles {
    entries: BTreeMap<String, Profile>,
    revision: String,
    radius_endpoint: Option<String>,
    operations: super::operation_models::Catalogue,
}

#[derive(Clone)]
struct Profile {
    provider: Provider,
    endpoint: String,
    api_key: Option<String>,
    headers: BTreeMap<String, String>,
    auth_header: bool,
    compat: ModelCompatibility,
    models: Vec<ConfiguredModel>,
    model_endpoints: BTreeMap<String, String>,
    overrides: BTreeMap<String, ModelMetadata>,
}

#[derive(Clone, Default)]
pub(super) struct RequestHeaders {
    values: BTreeMap<String, String>,
    auth_header: bool,
}

impl RequestHeaders {
    pub fn has_command(&self) -> bool {
        self.values.values().any(|value| value.starts_with('!'))
    }

    pub fn static_headers(&self, provider: &Provider, token: Option<&str>) -> Result<HeaderMap> {
        let mut headers = HeaderMap::with_capacity(self.values.len());
        for (name, value) in &self.values {
            let value = expand_value(value)
                .ok_or_else(|| Error::Config(format!("could not resolve model header {name}")))?;
            Self::insert(&mut headers, name, &value)?;
        }
        self.authenticate(headers, provider, token)
    }

    pub async fn resolve(&self, provider: &Provider, token: Option<&str>) -> Result<HeaderMap> {
        let mut headers = HeaderMap::with_capacity(self.values.len());
        for (name, value) in &self.values {
            let resolved = if value.starts_with('!') {
                pi_rs_workbench::mcp::resolve_value(value)
                    .await
                    .ok()
                    .filter(|value| !value.trim().is_empty())
            } else {
                expand_value(value)
            };
            let value = resolved
                .ok_or_else(|| Error::Config(format!("could not resolve model header {name}")))?;
            Self::insert(&mut headers, name, &value)?;
        }
        self.authenticate(headers, provider, token)
    }

    fn insert(headers: &mut HeaderMap, name: &str, value: &str) -> Result<()> {
        headers.insert(
            HeaderName::from_bytes(name.as_bytes()).expect("validated header name"),
            sensitive_header(name, value)?,
        );
        Ok(())
    }

    fn authenticate(
        &self,
        mut headers: HeaderMap,
        provider: &Provider,
        token: Option<&str>,
    ) -> Result<HeaderMap> {
        if self.auth_header {
            match token {
                Some(token) => Self::insert(
                    &mut headers,
                    AUTHORIZATION.as_str(),
                    &format!("Bearer {token}"),
                )?,
                None if provider == &Provider::Radius => {
                    // The gateway adapter adds its current OAuth access token.
                    headers.remove(AUTHORIZATION);
                }
                None => {
                    return Err(Error::Config(
                        "authHeader requires a resolved API key".into(),
                    ));
                }
            }
        }
        Ok(headers)
    }
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
    compat: ModelCompatibility,
    #[serde(default)]
    models: Vec<ConfiguredModel>,
    #[serde(default)]
    model_overrides: BTreeMap<String, ModelMetadata>,
}

#[derive(Clone, Deserialize)]
struct ConfiguredModel {
    id: String,
    #[serde(rename = "type")]
    kind: Option<String>,
    #[serde(rename = "baseUrl")]
    base_url: Option<String>,
    api: Option<String>,
    #[serde(flatten)]
    metadata: ModelMetadata,
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum AffinityFormat {
    Openrouter,
    Openai,
    OpenaiNosession,
}

#[derive(Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ModelCompatibility {
    #[serde(flatten)]
    chat: ChatCompatibility,
    allow_empty_signature: Option<bool>,
    force_adaptive_thinking: Option<bool>,
    send_session_affinity_headers: Option<bool>,
    session_affinity_format: Option<AffinityFormat>,
}

impl ModelCompatibility {
    fn overlay(self, newer: Self) -> Self {
        Self {
            chat: self.chat.overlay(newer.chat),
            allow_empty_signature: newer.allow_empty_signature.or(self.allow_empty_signature),
            force_adaptive_thinking: newer
                .force_adaptive_thinking
                .or(self.force_adaptive_thinking),
            send_session_affinity_headers: newer
                .send_session_affinity_headers
                .or(self.send_session_affinity_headers),
            session_affinity_format: newer
                .session_affinity_format
                .or(self.session_affinity_format),
        }
    }
}

/// Model prices in dollars per million tokens.
#[derive(Clone, Default, Deserialize)]
struct ChatRates {
    input: Option<f64>,
    output: Option<f64>,
    #[serde(rename = "cacheRead")]
    cache_read: Option<f64>,
    #[serde(rename = "cacheWrite")]
    cache_write: Option<f64>,
}

impl ChatRates {
    fn overlay(self, newer: Self) -> Self {
        Self {
            input: newer.input.or(self.input),
            output: newer.output.or(self.output),
            cache_read: newer.cache_read.or(self.cache_read),
            cache_write: newer.cache_write.or(self.cache_write),
        }
    }

    fn valid(&self) -> bool {
        [self.input, self.output, self.cache_read, self.cache_write]
            .into_iter()
            .flatten()
            .all(|amount| amount.is_finite() && amount >= 0.0)
    }

    fn value(&self) -> Value {
        serde_json::json!({
            "input": self.input.unwrap_or_default(),
            "output": self.output.unwrap_or_default(),
            "cacheRead": self.cache_read.unwrap_or_default(),
            "cacheWrite": self.cache_write.unwrap_or_default(),
        })
    }
}

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PriceTier {
    input_tokens_above: u64,
    #[serde(flatten)]
    rates: ChatRates,
}

#[derive(Clone, Default, Deserialize)]
pub(super) struct ChatPrice {
    #[serde(flatten)]
    rates: ChatRates,
    tiers: Option<Vec<PriceTier>>,
}

impl ChatPrice {
    fn overlay(self, newer: Self) -> Self {
        Self {
            rates: self.rates.overlay(newer.rates),
            tiers: newer.tiers.or(self.tiers),
        }
    }

    fn valid(&self) -> bool {
        self.rates.valid()
            && self
                .tiers
                .as_deref()
                .unwrap_or_default()
                .iter()
                .all(|tier| tier.rates.valid())
    }

    pub fn value(&self) -> Value {
        let mut value = self.rates.value();
        if let Some(tiers) = &self.tiers {
            value["tiers"] = Value::Array(
                tiers
                    .iter()
                    .map(|tier| {
                        let mut value = tier.rates.value();
                        value["inputTokensAbove"] = serde_json::json!(tier.input_tokens_above);
                        value
                    })
                    .collect(),
            );
        }
        value
    }

    pub fn estimate(
        &self,
        usage: pi_rs::Usage,
        metadata: &pi_rs::ModelMetadata,
    ) -> Option<pi_rs::UsageCostBreakdown> {
        let tier = self
            .tiers
            .as_deref()
            .unwrap_or_default()
            .iter()
            .rev()
            .filter(|tier| usage.input_tokens > tier.input_tokens_above)
            .max_by_key(|tier| tier.input_tokens_above);
        let rates = tier.map_or(&self.rates, |tier| &tier.rates);
        let reads = metadata
            .cached_input_tokens
            .unwrap_or_default()
            .min(usage.input_tokens);
        let other_input = usage.input_tokens - reads;
        let writes = metadata
            .cache_creation_input_tokens
            .unwrap_or_default()
            .min(other_input);
        let long = metadata
            .cache_write_1h_tokens
            .unwrap_or_default()
            .min(writes);
        let input_rate = rates.input.unwrap_or_default();
        pi_rs::UsageCostBreakdown::new(
            (input_rate / 1_000_000.0) * (other_input - writes) as f64,
            (rates.output.unwrap_or_default() / 1_000_000.0) * usage.output_tokens as f64,
            (rates.cache_read.unwrap_or_default() / 1_000_000.0) * reads as f64,
            (rates.cache_write.unwrap_or_default() / 1_000_000.0) * (writes - long) as f64
                + (input_rate / 500_000.0) * long as f64,
        )
    }
}

#[derive(Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct ModelMetadata {
    pub name: Option<String>,
    pub context_window: Option<u32>,
    pub max_tokens: Option<u32>,
    pub reasoning: Option<bool>,
    pub cost: Option<ChatPrice>,
    #[serde(default)]
    compat: ModelCompatibility,
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
        self.compat = self.compat.overlay(overrides.compat.clone());
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
            cost: match (self.cost, overrides.cost.clone()) {
                (Some(base), Some(newer)) => Some(base.overlay(newer)),
                (base, newer) => newer.or(base),
            },
            input: overrides.input.clone().or(self.input),
            ..self
        }
    }

    fn validate(&self, provider: &Provider, adapter: Adapter, id: &str) -> Result<()> {
        if id.trim().is_empty()
            || self
                .name
                .as_ref()
                .is_some_and(|name| name.trim().is_empty())
            || self.context_window == Some(0)
            || self.max_tokens == Some(0)
            || self.cost.as_ref().is_some_and(|cost| !cost.valid())
        {
            return Err(Error::Config(format!(
                "invalid model metadata for {}/{id}",
                provider.id()
            )));
        }
        if adapter == Adapter::ChatCompletions {
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

    pub fn levels(&self, adapter: Adapter) -> Option<Vec<String>> {
        if self.reasoning == Some(false) {
            return Some(vec!["off".into()]);
        }
        if !matches!(adapter, Adapter::ChatCompletions | Adapter::Anthropic)
            || self.thinking_level_map.is_empty()
        {
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

    pub fn allows_empty_signature(&self) -> bool {
        self.compat.allow_empty_signature == Some(true)
    }

    pub fn adaptive_thinking(&self) -> Option<bool> {
        self.compat.force_adaptive_thinking
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

    pub fn apply(&self, adapter: Adapter, details: &mut pi_rs::ollama::ModelDetails) {
        details.context_window = self.context_window.or(details.context_window);
        details.max_output_tokens = self.max_tokens.or(details.max_output_tokens);
        details.reasoning = self.reasoning.or(details.reasoning);
        if let Some(input) = &self.input {
            details.images = Some(input.contains(&Modality::Image));
        }
        if let Some(levels) = self.levels(adapter) {
            details.thinking_levels = Some(levels);
        }
    }
}

static BUILTIN_MODELS: LazyLock<BTreeMap<String, Vec<ConfiguredModel>>> = LazyLock::new(|| {
    serde_json::from_str(include_str!("../assets/builtin-chat-models.json"))
        .expect("bundled Pi chat models")
});

impl Profiles {
    pub fn load() -> Result<Self> {
        let path = config::agent_dir().join("models.json");
        let data = config::read_object(&path)?;
        let mut profiles = Self::parse(&data)?;
        if std::env::var_os("PI_RADIUS_GATEWAY").is_some() {
            let endpoint = super::radius_auth::model_endpoint(
                &super::radius_auth::Gateway::configured_origin()?,
            )
            .to_string();
            if let Some(profile) = profiles.entries.get_mut("radius")
                && data["providers"].as_object().is_some_and(|providers| {
                    providers.iter().any(|(name, entry)| {
                        name.eq_ignore_ascii_case("radius") && entry["baseUrl"].is_null()
                    })
                })
            {
                profile.endpoint.clone_from(&endpoint);
            }
            profiles.radius_endpoint = Some(endpoint);
        }
        Ok(profiles)
    }

    fn parse(data: &Value) -> Result<Self> {
        let empty = serde_json::Map::new();
        let providers = match data.get("providers") {
            Some(providers) => providers
                .as_object()
                .ok_or_else(|| Error::Config("models.json providers must be an object".into()))?,
            None => &empty,
        };
        let operations = super::operation_models::Catalogue::parse(providers)?;
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
                |base| endpoint_from_base(&provider, provider.adapter(), base),
            )?;
            profile
                .models
                .retain(|model| model.kind.as_deref().is_none_or(|kind| kind == "chat"));
            let mut ids = std::collections::BTreeSet::new();
            let mut model_endpoints = BTreeMap::new();
            for model in &mut profile.models {
                let adapter = match model.api.as_deref() {
                    None => provider.adapter(),
                    Some(api) => Adapter::named(api).ok_or_else(|| {
                        Error::Config(format!(
                            "unsupported API {api:?} for model {}/{}",
                            provider.id(),
                            model.id
                        ))
                    })?,
                };
                if let Some(base) = model.base_url.as_deref().or_else(|| {
                    profile
                        .base_url
                        .as_deref()
                        .filter(|_| adapter != provider.adapter())
                }) {
                    model_endpoints.insert(
                        model.id.clone(),
                        endpoint_from_base(&provider, adapter, base)?,
                    );
                } else if adapter != provider.adapter() {
                    let endpoint = builtin_chat::find(provider.id())
                        .and_then(|builtin| builtin.endpoint_for(adapter))
                        .ok_or_else(|| {
                            Error::Config(format!(
                                "model {}/{} requires baseUrl for the {} API",
                                provider.id(),
                                model.id,
                                adapter.api()
                            ))
                        })?;
                    model_endpoints.insert(model.id.clone(), endpoint.into());
                }
                model.metadata.validate(&provider, adapter, &model.id)?;
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
            if let Some(builtin) = builtin_chat::find(provider.id())
                && let Some(models) = BUILTIN_MODELS.get(provider.id())
            {
                for model in models
                    .iter()
                    .filter(|model| !ids.contains(model.id.as_str()))
                {
                    let Some(adapter) = model.api.as_deref().and_then(Adapter::named) else {
                        continue;
                    };
                    let Some(official) = builtin.endpoint_for(adapter) else {
                        continue;
                    };
                    let endpoint = profile.base_url.as_deref().map_or_else(
                        || Ok(official.to_owned()),
                        |base| endpoint_from_base(&provider, adapter, base),
                    )?;
                    model_endpoints.insert(model.id.clone(), endpoint);
                }
            }
            for (id, metadata) in &mut profile.model_overrides {
                let adapter = profile
                    .models
                    .iter()
                    .find(|model| model.id == *id)
                    .and_then(|model| model.api.as_deref())
                    .and_then(Adapter::named)
                    .unwrap_or_else(|| provider.adapter());
                metadata.validate(&provider, adapter, id)?;
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
                        compat: profile.compat,
                        models: profile.models,
                        model_endpoints,
                        overrides: profile.model_overrides,
                    },
                )
                .is_some()
            {
                return Err(Error::Config(format!("duplicate model provider {id}")));
            }
        }
        let revision = format!("{:x}", Sha256::digest(serde_json::to_vec(data)?));
        Ok(Self {
            entries,
            revision,
            radius_endpoint: None,
            operations,
        })
    }

    pub fn operations(&self) -> &super::operation_models::Catalogue {
        &self.operations
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
        Provider::ALL
            .into_iter()
            .chain(
                builtin_chat::PROVIDERS
                    .iter()
                    .map(|builtin| builtin.provider()),
            )
            .chain(
                self.entries
                    .values()
                    .filter(|profile| {
                        matches!(profile.provider, Provider::Custom { .. })
                            && builtin_chat::find(profile.provider.id()).is_none()
                    })
                    .map(|profile| profile.provider.clone()),
            )
    }

    pub fn endpoint(&self, provider: &Provider) -> &str {
        self.entries.get(provider.id()).map_or_else(
            || {
                if provider == &Provider::Radius {
                    self.radius_endpoint
                        .as_deref()
                        .unwrap_or_else(|| provider.default_endpoint())
                } else {
                    provider.default_endpoint()
                }
            },
            |profile| &profile.endpoint,
        )
    }

    pub fn model_endpoint(&self, provider: &Provider, model: &str) -> Option<&str> {
        if let Some(profile) = self.entries.get(provider.id()) {
            return profile.model_endpoints.get(model).map(String::as_str);
        }
        let adapter = self.model_adapter(provider, model);
        (adapter != provider.adapter())
            .then(|| builtin_chat::find(provider.id())?.endpoint_for(adapter))
            .flatten()
    }

    pub fn credential(&self, provider: &Provider, endpoint: &str) -> Option<Credential> {
        let profile = self.entries.get(provider.id())?;
        if profile.endpoint != endpoint
            && !profile
                .model_endpoints
                .values()
                .any(|value| value == endpoint)
        {
            return None;
        }
        Credential::configured(profile.api_key.as_deref()?)
    }

    pub fn header_settings(
        &self,
        provider: &Provider,
        endpoint: &str,
        model: Option<&str>,
    ) -> RequestHeaders {
        let Some(profile) = self.entries.get(provider.id()).filter(|profile| {
            profile.endpoint == endpoint
                || model
                    .and_then(|id| profile.model_endpoints.get(id))
                    .is_some_and(|value| value == endpoint)
        }) else {
            return RequestHeaders::default();
        };
        let mut configured = profile.headers.clone();
        if let Some(metadata) = model.and_then(|id| self.model(provider, id)) {
            configured.extend(metadata.headers);
        }
        RequestHeaders {
            values: configured,
            auth_header: profile.auth_header,
        }
    }

    #[cfg(test)]
    fn request_headers(
        &self,
        provider: &Provider,
        endpoint: &str,
        model: Option<&str>,
        token: Option<&str>,
    ) -> Result<HeaderMap> {
        self.header_settings(provider, endpoint, model)
            .static_headers(provider, token)
    }

    pub fn chat_compatibility(
        &self,
        provider: &Provider,
        model: Option<&ModelMetadata>,
    ) -> ChatCompatibility {
        let builtin = builtin_chat::find(provider.id())
            .map(|builtin| {
                ChatCompatibility::deserialize(builtin.compatibility())
                    .expect("bundled Pi chat compatibility")
            })
            .unwrap_or_default();
        let profile = self
            .entries
            .get(provider.id())
            .map(|profile| profile.compat.chat.clone())
            .unwrap_or_default();
        builtin.overlay(profile).overlay(
            model
                .map(|model| model.compat.chat.clone())
                .unwrap_or_default(),
        )
    }

    pub fn session_affinity(
        &self,
        provider: &Provider,
        model: &str,
        endpoint: &str,
    ) -> &'static [&'static str] {
        let adapter = self.model_adapter(provider, model);
        if !matches!(adapter, Adapter::Anthropic | Adapter::ChatCompletions) {
            return &[];
        }
        let profile = self
            .entries
            .get(provider.id())
            .map(|profile| &profile.compat);
        let model = self.model(provider, model);
        let model = model.as_ref().map(|model| &model.compat);
        let openrouter = provider.id() == "openrouter"
            || reqwest::Url::parse(endpoint)
                .ok()
                .is_some_and(|url| url.host_str() == Some("openrouter.ai"));
        let send = model
            .and_then(|compat| compat.send_session_affinity_headers)
            .or_else(|| profile.and_then(|compat| compat.send_session_affinity_headers))
            .unwrap_or(openrouter);
        if !send {
            return &[];
        }
        let format = model
            .and_then(|compat| compat.session_affinity_format)
            .or_else(|| profile.and_then(|compat| compat.session_affinity_format))
            .unwrap_or(if openrouter {
                AffinityFormat::Openrouter
            } else {
                AffinityFormat::Openai
            });
        match (adapter, format) {
            (_, AffinityFormat::Openrouter) => &["x-session-id"],
            (Adapter::Anthropic, _) => &["x-session-affinity"],
            (_, AffinityFormat::Openai) => {
                &["session_id", "x-client-request-id", "x-session-affinity"]
            }
            (_, AffinityFormat::OpenaiNosession) => &["x-client-request-id", "x-session-affinity"],
        }
    }

    pub fn model_ids(&self, provider: &Provider) -> Vec<String> {
        let mut ids: Vec<_> = self
            .entries
            .get(provider.id())
            .into_iter()
            .flat_map(|profile| profile.models.iter().map(|model| model.id.clone()))
            .collect();
        if let Some(models) = BUILTIN_MODELS.get(provider.id()) {
            let configured: std::collections::BTreeSet<_> = ids.iter().cloned().collect();
            ids.extend(
                models
                    .iter()
                    .filter(|model| !configured.contains(&model.id))
                    .map(|model| model.id.clone()),
            );
        }
        ids
    }

    fn definition(&self, provider: &Provider, id: &str) -> Option<&ConfiguredModel> {
        self.entries
            .get(provider.id())
            .and_then(|profile| profile.models.iter().find(|model| model.id == id))
            .or_else(|| {
                BUILTIN_MODELS
                    .get(provider.id())?
                    .iter()
                    .find(|model| model.id == id)
            })
    }

    pub fn model_adapter(&self, provider: &Provider, model: &str) -> Adapter {
        self.definition(provider, model)
            .and_then(|model| model.api.as_deref())
            .and_then(Adapter::named)
            .unwrap_or_else(|| provider.adapter())
    }

    pub fn model(&self, provider: &Provider, id: &str) -> Option<ModelMetadata> {
        let profile = self.entries.get(provider.id());
        let model = self
            .definition(provider, id)
            .map(|model| model.metadata.clone().with_model_defaults());
        match profile.and_then(|profile| profile.overrides.get(id)) {
            Some(overrides) => Some(model.unwrap_or_default().overlay(overrides)),
            None => model,
        }
    }
}

fn endpoint_from_base(provider: &Provider, adapter: Adapter, base: &str) -> Result<String> {
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

    let suffix = match adapter {
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
    } else if path.is_empty()
        && provider.id() == "deepseek"
        && url.host_str() == Some("api.deepseek.com")
    {
        suffix.to_owned()
    } else if path.is_empty() && adapter != Adapter::PiMessages {
        format!("/v1{suffix}")
    } else {
        format!("{path}{suffix}")
    };
    url.set_path(&path);
    Ok(url.to_string())
}

pub(super) fn expand_value(value: &str) -> Option<String> {
    if value.starts_with('!') {
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
        args.activate_provider(Provider::Anthropic, None).unwrap();
        assert_eq!(args.model_scope[0].provider.id(), "ollama");
        assert_eq!(args.endpoint, "http://127.0.0.1:7001/v1/messages");
        assert_eq!(args.model_token().as_deref(), Some("anthropic-profile"));
        assert!(
            args.model_credential_for(&Provider::Anthropic, "http://127.0.0.1:7002/v1/messages")
                .is_none()
        );
        args.activate_provider(Provider::Ollama, None).unwrap();
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
        custom.apply(Adapter::ChatCompletions, &mut details);
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
        let levels = model.levels(Adapter::ChatCompletions).unwrap();
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
