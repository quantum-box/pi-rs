//! User-level connection profiles for the installed provider adapters.
use std::collections::BTreeMap;
use std::sync::{Arc, LazyLock};

use pi_rs::ollama::ChatCompatibility;
use pi_rs::{Error, Result};
use pi_rs_workbench::image_profile::ImageResizeProfile;
use regex::{Captures, Regex};
use reqwest::header::{AUTHORIZATION, HeaderMap, HeaderName, HeaderValue};
use serde::{Deserialize, Serialize};
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
    env: Option<serde_json::Map<String, Value>>,
}

impl RequestHeaders {
    pub fn with_environment(mut self, env: Option<&serde_json::Map<String, Value>>) -> Self {
        self.env = env.cloned();
        self
    }
    pub fn has_command(&self) -> bool {
        self.values.values().any(|value| value.starts_with('!'))
    }

    pub fn static_headers(&self, provider: &Provider, token: Option<&str>) -> Result<HeaderMap> {
        let mut headers = HeaderMap::with_capacity(self.values.len());
        for (name, value) in &self.values {
            let value = expand_value_with_env(value, self.env.as_ref())
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
                expand_value_with_env(value, self.env.as_ref())
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

fn custom_adapter(entry: &Value) -> Option<Adapter> {
    if let Some(api) = entry.get("api").filter(|api| !api.is_null()) {
        return api.as_str().and_then(Adapter::named);
    }
    entry["models"]
        .as_array()?
        .iter()
        .filter(|model| model["type"].as_str().is_none_or(|kind| kind == "chat"))
        .find_map(|model| model["api"].as_str().and_then(Adapter::named))
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

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "lowercase")]
enum CacheControlFormat {
    Anthropic,
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
    supports_long_cache_retention: Option<bool>,
    supports_cache_control_on_tools: Option<bool>,
    cache_control_format: Option<CacheControlFormat>,
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
            supports_long_cache_retention: newer
                .supports_long_cache_retention
                .or(self.supports_long_cache_retention),
            supports_cache_control_on_tools: newer
                .supports_cache_control_on_tools
                .or(self.supports_cache_control_on_tools),
            cache_control_format: newer.cache_control_format.or(self.cache_control_format),
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

#[derive(Clone, Copy, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct InputLimits {
    #[serde(skip_serializing_if = "Option::is_none")]
    max_request_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    images: Option<ImageLimits>,
}

impl InputLimits {
    fn overlay(self, newer: Self) -> Self {
        Self {
            max_request_bytes: newer.max_request_bytes.or(self.max_request_bytes),
            images: match (self.images, newer.images) {
                (Some(base), Some(newer)) => Some(base.overlay(newer)),
                (base, newer) => newer.or(base),
            },
        }
    }

    fn valid(self) -> bool {
        self.max_request_bytes != Some(0) && self.images.is_none_or(ImageLimits::valid)
    }
}

#[derive(Clone, Copy, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct ImageLimits {
    #[serde(skip_serializing_if = "Option::is_none")]
    resize: Option<ImageResizeProfile>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_per_message: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_per_request: Option<u64>,
}

impl ImageLimits {
    fn overlay(self, newer: Self) -> Self {
        Self {
            resize: match (self.resize, newer.resize) {
                (Some(base), Some(newer)) => Some(base.overlay(newer)),
                (base, newer) => newer.or(base),
            },
            max_per_message: newer.max_per_message.or(self.max_per_message),
            max_per_request: newer.max_per_request.or(self.max_per_request),
        }
    }

    fn valid(self) -> bool {
        self.max_per_message != Some(0)
            && self.max_per_request != Some(0)
            && self.resize.is_none_or(ImageResizeProfile::valid)
    }
}

#[derive(Clone, Copy, Default, Deserialize, Serialize)]
struct PromptCache {
    #[serde(skip_serializing_if = "Option::is_none")]
    short: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    long: Option<f64>,
}

impl PromptCache {
    fn overlay(self, newer: Self) -> Self {
        Self {
            short: newer.short.or(self.short),
            long: newer.long.or(self.long),
        }
    }

    fn valid(self) -> bool {
        [self.short, self.long]
            .into_iter()
            .flatten()
            .all(|seconds| seconds.is_finite() && seconds > 0.0)
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
    input_limits: Option<InputLimits>,
    prompt_cache: Option<PromptCache>,
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
            input_limits: match (self.input_limits, overrides.input_limits) {
                (Some(base), Some(newer)) => Some(base.overlay(newer)),
                (base, newer) => newer.or(base),
            },
            prompt_cache: match (self.prompt_cache, overrides.prompt_cache) {
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
            || self.input_limits.is_some_and(|limits| !limits.valid())
            || self.prompt_cache.is_some_and(|cache| !cache.valid())
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
        if !matches!(
            adapter,
            Adapter::ChatCompletions | Adapter::Anthropic | Adapter::OpenAiResponses
        ) || self.thinking_level_map.is_empty()
        {
            return None;
        }
        Some(
            super::thinking::LEVELS
                .iter()
                .copied()
                .filter(|level| {
                    self.thinking_level_map
                        .get(*level)
                        .map_or(!matches!(*level, "xhigh" | "max"), Option::is_some)
                })
                .map(str::to_owned)
                .collect(),
        )
    }

    pub fn apply_catalog_metadata(&self, model: &mut Value) {
        if let Some(limits) = self.input_limits {
            model["inputLimits"] = serde_json::json!(limits);
        }
        if let Some(cache) = self.prompt_cache {
            model["promptCache"] = serde_json::json!(cache);
        }
        if let Some(cost) = &self.cost {
            model["cost"] = cost.value();
        }
    }

    pub fn image_resize(&self) -> Option<ImageResizeProfile> {
        self.input_limits.as_ref()?.images.as_ref()?.resize
    }

    pub fn supports_images(&self) -> Option<bool> {
        self.input
            .as_ref()
            .map(|input| input.contains(&Modality::Image))
    }

    pub fn prompt_cache_seconds(&self, retention: pi_rs::CacheRetention) -> Option<f64> {
        let cache = self.prompt_cache?;
        match retention {
            pi_rs::CacheRetention::None => None,
            pi_rs::CacheRetention::Short => cache.short,
            pi_rs::CacheRetention::Long => cache.long,
        }
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

    pub(super) fn parse(data: &Value) -> Result<Self> {
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
            let builtin = Provider::named(name);
            let provider = if let Some(provider) = &builtin {
                if provider.id() == "openai" && entry["api"] == "openai-completions" {
                    Provider::Custom {
                        id: Arc::from("openai"),
                        adapter: Adapter::ChatCompletions,
                    }
                } else {
                    provider.clone()
                }
            } else {
                let Some(adapter) = custom_adapter(entry) else {
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
                    None if builtin.is_none() && profile.api.is_none() => {
                        return Err(Error::Config(format!(
                            "model {}/{} requires an API when the provider omits api",
                            provider.id(),
                            model.id
                        )));
                    }
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
                && !(provider.id() == "openai" && provider.adapter() != Adapter::OpenAiResponses)
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
        self.entries
            .get(&name.to_ascii_lowercase())
            .map(|profile| profile.provider.clone())
            .or_else(|| Provider::named(name))
    }

    pub fn providers(&self) -> impl Iterator<Item = Provider> + '_ {
        Provider::ALL
            .into_iter()
            .chain(std::iter::once(
                Provider::named("openai-chatgpt").expect("ChatGPT provider"),
            ))
            .chain(builtin_chat::PROVIDERS.iter().map(|builtin| {
                let provider = builtin.provider();
                self.entries
                    .get(provider.id())
                    .map_or(provider.clone(), |profile| profile.provider.clone())
            }))
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

    pub fn has_endpoint(&self, provider: &Provider, endpoint: &str) -> bool {
        self.entries.get(provider.id()).is_some_and(|profile| {
            profile.endpoint == endpoint
                || profile
                    .model_endpoints
                    .values()
                    .any(|value| value == endpoint)
        })
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
            let values = builtin_chat::find(provider.id())
                .filter(|builtin| builtin.is_official_endpoint(endpoint))
                .and_then(|_| BUILTIN_MODELS.get(provider.id()))
                .and_then(|models| models.iter().find(|item| Some(item.id.as_str()) == model))
                .map_or_else(BTreeMap::new, |item| item.metadata.headers.clone());
            return RequestHeaders {
                values,
                ..RequestHeaders::default()
            };
        };
        let mut configured = profile.headers.clone();
        if let Some(metadata) = model.and_then(|id| self.model(provider, id)) {
            configured.extend(metadata.headers);
        }
        RequestHeaders {
            values: configured,
            auth_header: profile.auth_header,
            env: None,
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

    fn cache_compatibility(
        &self,
        provider: &Provider,
        model: Option<&ModelMetadata>,
    ) -> ModelCompatibility {
        let base = self
            .entries
            .get(provider.id())
            .map(|profile| profile.compat.clone())
            .unwrap_or_default();
        match model {
            Some(model) => base.overlay(model.compat.clone()),
            None => base,
        }
    }

    pub fn messages_cache_support(
        &self,
        provider: &Provider,
        model: Option<&ModelMetadata>,
    ) -> (bool, bool) {
        let compat = self.cache_compatibility(provider, model);
        (
            compat.supports_long_cache_retention.unwrap_or(true),
            compat.supports_cache_control_on_tools.unwrap_or(true),
        )
    }

    pub fn chat_cache_support(
        &self,
        provider: &Provider,
        model_id: &str,
        model: Option<&ModelMetadata>,
    ) -> Option<bool> {
        let compat = self.cache_compatibility(provider, model);
        let enabled = compat.cache_control_format.is_some()
            || (provider.id() == "openrouter" && model_id.starts_with("anthropic/"));
        enabled.then(|| {
            compat.supports_long_cache_retention.unwrap_or_else(|| {
                !matches!(
                    provider.id(),
                    "together"
                        | "cloudflare-workers-ai"
                        | "cloudflare-ai-gateway"
                        | "nvidia"
                        | "ant-ling"
                )
            })
        })
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
        if let Some(models) = BUILTIN_MODELS.get(provider.id())
            && !(provider.id() == "openai" && provider.adapter() != Adapter::OpenAiResponses)
        {
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
                if provider.id() == "openai" && provider.adapter() != Adapter::OpenAiResponses {
                    return None;
                }
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
        Adapter::OpenAiResponses => "/responses",
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
    expand_value_with_env(value, None)
}

pub(super) fn expand_value_with_env(
    value: &str,
    env: Option<&serde_json::Map<String, Value>>,
) -> Option<String> {
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
        env.and_then(|env| env.get(name.as_str()))
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .map(str::to_owned)
            .or_else(|| {
                std::env::var(name.as_str())
                    .ok()
                    .filter(|value| !value.is_empty())
            })
            .unwrap_or_else(|| {
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
    fn vercel_gateway_catalog_and_endpoint() {
        let profiles = Profiles::parse(&json!({})).unwrap();
        let provider = profiles.provider("vercel-ai-gateway").unwrap();
        assert_eq!(provider.adapter(), Adapter::Anthropic);
        assert_eq!(
            profiles.endpoint(&provider),
            "https://ai-gateway.vercel.sh/v1/messages"
        );
        assert_eq!(
            builtin_chat::find(provider.id()).unwrap().env(),
            "AI_GATEWAY_API_KEY"
        );
        assert_eq!(profiles.model_ids(&provider).len(), 303);
        let model = profiles
            .model(&provider, "anthropic/claude-sonnet-4.6")
            .unwrap();
        assert!(model.allows_empty_signature());
        assert_eq!(model.adaptive_thinking(), Some(true));
        assert_eq!(model.context_window, Some(1_000_000));
        let profiles = Profiles::parse(
            &json!({"providers":{"vercel-ai-gateway":{"baseUrl":"http://localhost:4321"}}}),
        )
        .unwrap();
        let provider = profiles.provider("vercel-ai-gateway").unwrap();
        assert_eq!(
            profiles.model_endpoint(&provider, "anthropic/claude-sonnet-4.6"),
            Some("http://localhost:4321/v1/messages")
        );
        assert!(
            !builtin_chat::find(provider.id())
                .unwrap()
                .is_official_endpoint("http://localhost:4321/v1/messages")
        );
    }

    #[test]
    fn xai_catalog_uses_responses_and_model_specific_thinking_levels() {
        let profiles = Profiles::parse(&json!({})).unwrap();
        let provider = profiles.provider("xai").unwrap();
        assert_eq!(provider.adapter(), Adapter::OpenAiResponses);
        assert_eq!(
            profiles.endpoint(&provider),
            "https://api.x.ai/v1/responses"
        );
        assert_eq!(builtin_chat::find("xai").unwrap().env(), "XAI_API_KEY");
        assert_eq!(profiles.model_ids(&provider).len(), 4);
        let model = profiles.model(&provider, "grok-4.3").unwrap();
        assert!(
            model
                .levels(Adapter::OpenAiResponses)
                .unwrap()
                .contains(&"off".into())
        );
        let model = profiles.model(&provider, "grok-4.7").unwrap();
        assert!(
            !model
                .levels(Adapter::OpenAiResponses)
                .unwrap()
                .contains(&"off".into())
        );
    }

    #[test]
    fn openai_api_key_provider_includes_upstream_responses_models() {
        let profiles = Profiles::parse(&json!({})).unwrap();
        let provider = profiles.provider("openai").unwrap();
        assert_eq!(provider.adapter(), Adapter::OpenAiResponses);
        assert_eq!(
            profiles.endpoint(&provider),
            "https://api.openai.com/v1/responses"
        );
        assert!(
            profiles
                .model_ids(&provider)
                .contains(&"gpt-4.1".to_owned())
        );
        assert_eq!(
            profiles.model(&provider, "gpt-4.1").unwrap().reasoning,
            Some(false)
        );
        let reasoning = profiles.model(&provider, "gpt-5").unwrap();
        let levels = reasoning.levels(Adapter::OpenAiResponses).unwrap();
        assert!(levels.contains(&"high".to_owned()));
        assert!(!levels.contains(&"off".to_owned()));
        assert_eq!(reasoning.mapped_effort("high"), Some(Some("high")));
    }

    #[test]
    fn existing_openai_completions_profile_preserves_its_adapter_and_models() {
        let profiles = Profiles::parse(&json!({"providers":{"openai":{
            "api":"openai-completions", "baseUrl":"https://relay.example/v1",
            "models":[{"id":"relay-model"}]
        }}}))
        .unwrap();
        let provider = profiles.provider("openai").unwrap();
        assert_eq!(provider.adapter(), Adapter::ChatCompletions);
        assert_eq!(
            profiles.endpoint(&provider),
            "https://relay.example/v1/chat/completions"
        );
        assert_eq!(profiles.model_ids(&provider), vec!["relay-model"]);
        assert!(profiles.providers().any(|candidate| candidate == provider));
    }

    #[test]
    fn ant_ling_catalog_and_credential_match_reference() {
        let profiles = Profiles::parse(&json!({})).unwrap();
        let provider = profiles.provider("ant-ling").unwrap();
        assert_eq!(provider.adapter(), Adapter::ChatCompletions);
        assert_eq!(
            profiles.endpoint(&provider),
            "https://api.ant-ling.com/v1/chat/completions"
        );
        assert_eq!(
            builtin_chat::find("ant-ling").unwrap().env(),
            "ANT_LING_API_KEY"
        );
        assert_eq!(profiles.model_ids(&provider).len(), 3);
        let ring = profiles.model(&provider, "Ring-2.6-1T").unwrap();
        assert_eq!(
            ring.levels(Adapter::ChatCompletions).unwrap(),
            ["high", "xhigh"]
        );
        assert_eq!(ring.mapped_effort("high"), Some(Some("high")));
        assert_eq!(
            profiles
                .model(&provider, "Ling-2.6-flash")
                .unwrap()
                .reasoning,
            Some(false)
        );
    }

    #[test]
    fn kimi_coding_catalog_preserves_models_and_reasoning_levels() {
        let profiles = Profiles::parse(&json!({})).unwrap();
        let provider = profiles.provider("kimi-coding").unwrap();
        assert_eq!(provider.adapter(), Adapter::ChatCompletions);
        assert_eq!(
            profiles.endpoint(&provider),
            "https://api.kimi.com/coding/v1/chat/completions"
        );
        assert_eq!(
            builtin_chat::find("kimi-coding").unwrap().env(),
            "KIMI_API_KEY"
        );
        assert_eq!(profiles.model_ids(&provider).len(), 6);
        let model = profiles.model(&provider, "k3.1-pro").unwrap();
        assert_eq!(
            model.levels(Adapter::ChatCompletions).unwrap(),
            ["low", "high", "max"]
        );
        assert_eq!(model.supports_images(), Some(true));
        let fast = profiles
            .model(&provider, "kimi-for-coding-highspeed")
            .unwrap();
        assert!(
            !fast
                .levels(Adapter::ChatCompletions)
                .unwrap()
                .contains(&"off".into())
        );
    }

    #[test]
    fn qwen_token_plan_catalogs_keep_region_and_model_capabilities() {
        let profiles = Profiles::parse(&json!({})).unwrap();
        for (name, endpoint, key, count) in [
            (
                "qwen-token-plan",
                "https://token-plan.ap-southeast-1.maas.aliyuncs.com/compatible-mode/v1/chat/completions",
                "QWEN_TOKEN_PLAN_API_KEY",
                21,
            ),
            (
                "qwen-token-plan-cn",
                "https://token-plan.cn-beijing.maas.aliyuncs.com/compatible-mode/v1/chat/completions",
                "QWEN_TOKEN_PLAN_CN_API_KEY",
                21,
            ),
            (
                "qwen-token-plan-individual",
                "https://token-plan.ap-southeast-1.maas.aliyuncs.com/compatible-mode/v1/chat/completions",
                "QWEN_TOKEN_PLAN_API_KEY",
                9,
            ),
        ] {
            let provider = profiles.provider(name).unwrap();
            assert_eq!(provider.adapter(), Adapter::ChatCompletions);
            assert_eq!(profiles.endpoint(&provider), endpoint);
            assert_eq!(builtin_chat::find(name).unwrap().env(), key);
            assert_eq!(profiles.model_ids(&provider).len(), count);
            let model = profiles.model(&provider, "qwen3.8-max").unwrap();
            assert_eq!(model.supports_images(), Some(true));
            assert_eq!(
                model.levels(Adapter::ChatCompletions).unwrap(),
                ["low", "medium", "xhigh"]
            );
            assert!(profiles.model(&provider, "qwen3.8-max-preview").is_none());
        }
        assert!(
            profiles
                .model(
                    &profiles.provider("qwen-token-plan-individual").unwrap(),
                    "deepseek-v3.2"
                )
                .is_none()
        );
    }

    #[test]
    fn xiaomi_regional_catalogs_preserve_reasoning_and_credentials() {
        let profiles = Profiles::parse(&json!({})).unwrap();
        for (name, endpoint, variable, count) in [
            (
                "xiaomi",
                "https://api.xiaomimimo.com/v1/chat/completions",
                "XIAOMI_API_KEY",
                3,
            ),
            (
                "xiaomi-token-plan-cn",
                "https://token-plan-cn.xiaomimimo.com/v1/chat/completions",
                "XIAOMI_TOKEN_PLAN_CN_API_KEY",
                2,
            ),
            (
                "xiaomi-token-plan-ams",
                "https://token-plan-ams.xiaomimimo.com/v1/chat/completions",
                "XIAOMI_TOKEN_PLAN_AMS_API_KEY",
                2,
            ),
            (
                "xiaomi-token-plan-sgp",
                "https://token-plan-sgp.xiaomimimo.com/v1/chat/completions",
                "XIAOMI_TOKEN_PLAN_SGP_API_KEY",
                2,
            ),
        ] {
            let provider = profiles.provider(name).unwrap();
            assert_eq!(provider.adapter(), Adapter::ChatCompletions);
            assert_eq!(profiles.endpoint(&provider), endpoint);
            assert_eq!(builtin_chat::find(name).unwrap().env(), variable);
            assert_eq!(profiles.model_ids(&provider).len(), count);
            let model = profiles.model(&provider, "mimo-v2.6-pro").unwrap();
            assert_eq!(
                model.levels(Adapter::ChatCompletions).unwrap(),
                ["off", "high"]
            );
            assert_eq!(model.reasoning, Some(true));
        }
    }

    #[test]
    fn minimax_catalogs_use_native_messages_and_regional_credentials() {
        let profiles = Profiles::parse(&json!({})).unwrap();
        for (name, endpoint, variable) in [
            (
                "minimax",
                "https://api.minimax.io/anthropic/v1/messages",
                "MINIMAX_API_KEY",
            ),
            (
                "minimax-cn",
                "https://api.minimaxi.com/anthropic/v1/messages",
                "MINIMAX_CN_API_KEY",
            ),
        ] {
            let provider = profiles.provider(name).unwrap();
            assert_eq!(provider.adapter(), Adapter::Anthropic);
            assert_eq!(profiles.endpoint(&provider), endpoint);
            assert_eq!(builtin_chat::find(name).unwrap().env(), variable);
            assert_eq!(profiles.model_ids(&provider).len(), 2);
            let m3 = profiles.model(&provider, "MiniMax-M3").unwrap();
            assert_eq!(m3.levels(Adapter::Anthropic).unwrap(), ["off", "high"]);
            assert_eq!(m3.adaptive_thinking(), Some(true));
            let model = profiles.model(&provider, "MiniMax-M3.1-Flash").unwrap();
            assert_eq!(model.reasoning, Some(true));
            let levels = model.levels(Adapter::Anthropic).unwrap();
            assert!(levels.contains(&"high".into()));
            assert!(!levels.contains(&"off".into()));
        }
    }

    #[test]
    fn baseten_and_nvidia_builtin_models_keep_capabilities_and_headers() {
        let profiles = Profiles::parse(&json!({})).unwrap();
        let baseten = profiles.provider("baseten").unwrap();
        let nvidia = profiles.provider("nvidia").unwrap();
        assert_eq!(profiles.model_ids(&baseten).len(), 14);
        assert_eq!(profiles.model_ids(&nvidia).len(), 21);
        let model = profiles.model(&baseten, "zai-org/GLM-5.2").unwrap();
        assert_eq!(model.reasoning, Some(true));
        assert_eq!(
            model.thinking_level_map.get("high").unwrap().as_deref(),
            Some("high")
        );
        let endpoint = profiles.endpoint(&nvidia);
        let headers = profiles
            .request_headers(
                &nvidia,
                endpoint,
                Some("deepseek-ai/deepseek-v4.1-flash"),
                Some("fixture"),
            )
            .unwrap();
        assert_eq!(headers["nvcf-poll-seconds"], "3600");
        let untrusted = profiles
            .request_headers(
                &nvidia,
                "https://relay.example/v1/chat/completions",
                Some("deepseek-ai/deepseek-v4.1-flash"),
                None,
            )
            .unwrap();
        assert!(untrusted.get("nvcf-poll-seconds").is_none());
        let configured = Profiles::parse(&json!({"providers":{"nvidia":{
            "baseUrl":"https://relay.example/v1",
            "modelOverrides":{"deepseek-ai/deepseek-v4.1-flash":{"headers":{"X-Relay-Secret":"private"}}}
        }}})).unwrap();
        let official = configured
            .request_headers(
                &nvidia,
                endpoint,
                Some("deepseek-ai/deepseek-v4.1-flash"),
                None,
            )
            .unwrap();
        assert_eq!(official["nvcf-poll-seconds"], "3600");
        assert!(official.get("x-relay-secret").is_none());
        let relay = configured
            .request_headers(
                &nvidia,
                "https://relay.example/v1/chat/completions",
                Some("deepseek-ai/deepseek-v4.1-flash"),
                None,
            )
            .unwrap();
        assert_eq!(relay["x-relay-secret"], "private");
    }

    #[test]
    fn custom_profiles_can_declare_apis_only_on_their_models() {
        let data = json!({"providers":{"mixed":{
            "baseUrl":"https://relay.example/v1",
            "models":[{"id":"chat","api":"openai-completions"},{"id":"message","api":"anthropic-messages"}]
        }}});
        let profiles = Profiles::parse(&data).unwrap();
        let provider = profiles.provider("mixed").unwrap();
        assert_eq!(
            profiles.endpoint(&provider),
            "https://relay.example/v1/chat/completions"
        );
        assert_eq!(
            profiles.model_adapter(&provider, "message"),
            Adapter::Anthropic
        );
        assert_eq!(
            profiles.model_endpoint(&provider, "message"),
            Some("https://relay.example/v1/messages")
        );
        let mut missing = data;
        missing["providers"]["mixed"]["models"][1]
            .as_object_mut()
            .unwrap()
            .remove("api");
        assert!(
            matches!(Profiles::parse(&missing), Err(Error::Config(message)) if message.contains("mixed/message requires an API"))
        );
    }

    #[test]
    fn custom_provider_uses_endpointless_saved_key_only_for_configured_routes() {
        let mut args = super::super::Cli::parse_from(["pi"]);
        args.model_profiles = Profiles::parse(&json!({"providers":{"relay":{
            "api":"openai-completions", "baseUrl":"https://relay.example/v1",
            "apiKey":"profile-key", "models":[{"id":"model"}]
        }}}))
        .unwrap();
        args.model_keys = super::super::model_auth::Keys::parse(
            &json!({"relay":{"type":"api_key","key":"saved-key"}}),
        );
        let provider = args.model_profiles.provider("relay").unwrap();
        let endpoint = "https://relay.example/v1/chat/completions";
        assert_eq!(
            args.model_credential_for(&provider, endpoint)
                .unwrap()
                .literal(),
            Some("saved-key")
        );
        assert!(
            args.model_credential_for(&provider, "https://other.example/v1/chat/completions")
                .is_none()
        );
        args.model_keys = super::super::model_auth::Keys::parse(
            &json!({"relay":{"type":"api_key","key":"bound-key","endpoint":"https://other.example/v1/chat/completions"}}),
        );
        assert_eq!(
            args.model_credential_for(&provider, endpoint)
                .unwrap()
                .literal(),
            Some("profile-key")
        );
    }

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
            "models":[{"id":"custom","name":"Local Fixture","input":["text","image"],"maxTokens":200}, {"id":"text-only","input":["text"]}],
            "modelOverrides":{
                "custom":{"maxTokens":100},
                "existing":{"contextWindow":32000}
            }
        }}})).unwrap();
        assert_eq!(
            profiles.model_ids(&Provider::Ollama),
            ["custom", "text-only"]
        );
        assert_eq!(
            profiles
                .model(&Provider::Ollama, "text-only")
                .unwrap()
                .supports_images(),
            Some(false)
        );
        let custom = profiles.model(&Provider::Ollama, "custom").unwrap();
        assert_eq!(custom.supports_images(), Some(true));
        assert_eq!(custom.name.as_deref(), Some("Local Fixture"));
        assert_eq!(custom.max_tokens, Some(100));
        assert_eq!(custom.context_window, Some(128_000));
        let mut details = pi_rs::ollama::ModelDetails::default();
        custom.apply(Adapter::ChatCompletions, &mut details);
        assert_eq!(details.images, Some(true));
        assert_eq!(details.thinking_levels, Some(vec!["off".into()]));
        let existing = profiles.model(&Provider::Ollama, "existing").unwrap();
        assert_eq!(existing.supports_images(), None);
        assert_eq!(existing.context_window, Some(32000));
        assert_eq!(existing.max_tokens, None);
    }

    #[test]
    fn sampling_layers_merge_without_overriding_agent_fields() {
        let profiles = Profiles::parse(&json!({"providers":{"ollama":{
            "models":[{"id":"custom","reasoning":true,
                "thinkingLevelMap":{"off":null,"medium":null,"xhigh":"strong"},
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
                .any(|level| matches!(level.as_str(), "off" | "medium" | "max"))
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
    fn responses_sampling_is_checked_only_for_the_selected_model_and_level() {
        for key in [
            "model",
            "input",
            "stream",
            "max_output_tokens",
            "tools",
            "reasoning",
            "store",
        ] {
            for level_specific in [false, true] {
                for is_override in [false, true] {
                    let sampling = if level_specific {
                        json!({"samplingParamsByThinkingLevel":{"high":{(key): false}}})
                    } else {
                        json!({"samplingParams":{(key): false}})
                    };
                    let mut provider = json!({"models":[{"id":"fixture","reasoning":true}]});
                    if is_override {
                        provider["modelOverrides"] = json!({"fixture":sampling});
                    } else {
                        provider["models"][0]
                            .as_object_mut()
                            .unwrap()
                            .extend(sampling.as_object().unwrap().clone());
                    }
                    let profiles = Profiles::parse(&json!({"providers":{
                        "openai":provider,
                        "ollama":{"models":[{"id":"local"}]}
                    }}))
                    .unwrap();
                    let mut args = super::super::Cli::parse_from([
                        "pi",
                        "--provider",
                        "openai",
                        "--model",
                        "fixture",
                        "--thinking",
                        "high",
                    ]);
                    args.model_profiles = profiles;
                    args.provider = args.model_profiles.provider("openai").unwrap();
                    args.thinking = Some("high".into());
                    let client = super::super::providers::Client::OpenAi(
                        pi_rs::openai::OpenAiResponses::new_api(
                            "https://example.com/v1/responses",
                            "token".into(),
                        )
                        .unwrap(),
                    );
                    let error = match client.clone().with_model_options(&args, false) {
                        Ok(_) => {
                            panic!("accepted {key}, level={level_specific}, override={is_override}")
                        }
                        Err(error) => error,
                    };
                    let message = error.to_string();
                    assert!(
                        message.contains(key) && message.contains("openai/fixture"),
                        "{message}"
                    );
                    if level_specific {
                        args.thinking = Some("off".into());
                        assert!(client.with_model_options(&args, false).is_ok());
                    }
                    args.provider = Provider::Ollama;
                    args.model = "local".into();
                    let local = super::super::providers::Client::Ollama(
                        pi_rs::ollama::Ollama::new(
                            "http://127.0.0.1:11434/v1/chat/completions",
                            None,
                        )
                        .unwrap(),
                    );
                    assert!(local.with_model_options(&args, false).is_ok());
                }
            }
        }
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

    #[tokio::test]
    async fn stored_credential_environment_resolves_chat_headers_on_bound_endpoint() {
        let profiles = Profiles::parse(&json!({"providers":{"relay":{
            "api":"openai-completions", "baseUrl":"https://relay.example/v1",
            "headers":{"X-Credential":"$HEADER_VALUE"}, "models":[{"id":"chat"}]
        }}}))
        .unwrap();
        let provider = profiles.provider("relay").unwrap();
        let endpoint = profiles.endpoint(&provider);
        let keys = super::super::model_auth::Keys::parse(&json!({"relay":{
            "type":"api_key", "key":"$TOKEN", "env":{"TOKEN":"key-value", "HEADER_VALUE":"header-value"}
        }}));
        let credential = keys
            .custom_credential(&provider, endpoint, &profiles)
            .unwrap();
        assert_eq!(credential.resolve().await.unwrap(), "key-value");
        let settings = profiles
            .header_settings(&provider, endpoint, Some("chat"))
            .with_environment(keys.environment(&provider, endpoint, &profiles));
        assert_eq!(
            settings
                .static_headers(&provider, Some("key-value"))
                .unwrap()["x-credential"],
            "header-value"
        );
        assert_eq!(
            settings
                .resolve(&provider, Some("key-value"))
                .await
                .unwrap()["x-credential"],
            "header-value"
        );
        assert!(
            keys.environment(
                &provider,
                "https://other.example/v1/chat/completions",
                &profiles
            )
            .is_none()
        );
        let missing = super::super::model_auth::Keys::parse(
            &json!({"relay":{"type":"api_key", "env":{"HEADER_VALUE":"header-value"}}}),
        );
        assert!(
            missing
                .custom_credential(&provider, endpoint, &profiles)
                .is_none()
        );
        assert!(
            missing
                .environment(&provider, endpoint, &profiles)
                .is_some()
        );
    }
}
