//! User-level connection profiles for the installed provider adapters.
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, LazyLock};

use pi_rs::ollama::ChatCompatibility;
use pi_rs::{Error, Result};
use pi_rs_workbench::image_profile::ImageResizeProfile;
use reqwest::header::{AUTHORIZATION, HeaderMap, HeaderName, HeaderValue};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use super::{Provider, builtin_chat, config, model_auth::Credential, providers::Adapter};

#[derive(Clone, Default)]
pub(super) struct Profiles {
    entries: BTreeMap<String, Profile>,
    revision: String,
    cloudflare_saved_account: Option<String>,
    cloudflare_gateway_route: Option<super::cloudflare_gateway::Route>,
    radius_endpoint: Option<String>,
    operations: super::operation_models::Catalogue,
}

#[derive(Clone)]
struct Profile {
    provider: Provider,
    endpoint: String,
    base_url: Option<String>,
    configured_cloudflare_base: Option<String>,
    provider_endpoints: BTreeSet<String>,
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
    #[cfg(test)]
    pub(super) fn for_test(name: &str, value: &str) -> Self {
        Self {
            values: [(name.to_owned(), value.to_owned())].into_iter().collect(),
            ..Self::default()
        }
    }

    pub fn with_environment(mut self, env: Option<&serde_json::Map<String, Value>>) -> Self {
        self.env = env.cloned();
        self
    }
    pub fn environment(&self) -> Option<&serde_json::Map<String, Value>> {
        self.env.as_ref()
    }
    pub fn has_command(&self) -> bool {
        self.values.values().any(|value| value.starts_with('!'))
    }

    pub fn has_dynamic_values(&self) -> bool {
        self.values
            .values()
            .any(|value| value.starts_with('!') || value.contains('$'))
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

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "lowercase")]
enum PromptCacheKeyMode {
    Auto,
    Enabled,
    Disabled,
}

#[derive(Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ModelCompatibility {
    #[serde(flatten)]
    chat: ChatCompatibility,
    allow_empty_signature: Option<bool>,
    supports_strict_tools: Option<bool>,
    supports_max_output_tokens: Option<bool>,
    force_adaptive_thinking: Option<bool>,
    send_session_affinity_headers: Option<bool>,
    session_affinity_format: Option<AffinityFormat>,
    prompt_cache_key_mode: Option<PromptCacheKeyMode>,
    supports_long_cache_retention: Option<bool>,
    supports_explicit_prompt_cache_mode: Option<bool>,
    supports_cache_control_on_tools: Option<bool>,
    cache_control_format: Option<CacheControlFormat>,
}

impl ModelCompatibility {
    fn overlay(self, newer: Self) -> Self {
        Self {
            chat: self.chat.overlay(newer.chat),
            allow_empty_signature: newer.allow_empty_signature.or(self.allow_empty_signature),
            supports_strict_tools: newer.supports_strict_tools.or(self.supports_strict_tools),
            supports_max_output_tokens: newer
                .supports_max_output_tokens
                .or(self.supports_max_output_tokens),
            force_adaptive_thinking: newer
                .force_adaptive_thinking
                .or(self.force_adaptive_thinking),
            send_session_affinity_headers: newer
                .send_session_affinity_headers
                .or(self.send_session_affinity_headers),
            session_affinity_format: newer
                .session_affinity_format
                .or(self.session_affinity_format),
            prompt_cache_key_mode: newer.prompt_cache_key_mode.or(self.prompt_cache_key_mode),
            supports_long_cache_retention: newer
                .supports_long_cache_retention
                .or(self.supports_long_cache_retention),
            supports_explicit_prompt_cache_mode: newer
                .supports_explicit_prompt_cache_mode
                .or(self.supports_explicit_prompt_cache_mode),
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
        // Pi exposes the default five levels for a reasoning model even when
        // its configuration does not specify a thinkingLevelMap.
        if adapter == Adapter::Anthropic
            && self.reasoning == Some(true)
            && self.thinking_level_map.is_empty()
        {
            return Some(
                ["off", "minimal", "low", "medium", "high"]
                    .map(str::to_owned)
                    .to_vec(),
            );
        }
        if !matches!(
            adapter,
            Adapter::ChatCompletions
                | Adapter::MistralConversations
                | Adapter::Anthropic
                | Adapter::OpenAiResponses
                | Adapter::CodexResponses
                | Adapter::Google
                | Adapter::Bedrock
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
        if !self.thinking_level_map.is_empty() {
            model["thinkingLevelMap"] = serde_json::json!(self.thinking_level_map);
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

    pub fn input_modalities(&self) -> Option<Vec<&'static str>> {
        self.input.as_ref().map(|input| {
            input
                .iter()
                .map(|modality| match modality {
                    Modality::Text => "text",
                    Modality::Image => "image",
                })
                .collect()
        })
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

    pub fn has_thinking_level_map(&self) -> bool {
        !self.thinking_level_map.is_empty()
    }

    pub fn has_thinking_map(&self) -> bool {
        !self.thinking_level_map.is_empty()
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
    let mut models: BTreeMap<String, Vec<ConfiguredModel>> =
        serde_json::from_str(include_str!("../assets/builtin-chat-models.json"))
            .expect("bundled Pi chat models");
    // Pi's direct Anthropic catalog marks its Messages models as strict capable.
    if let Some(anthropic) = models.get_mut("anthropic") {
        for model in anthropic {
            if model
                .api
                .as_deref()
                .is_none_or(|api| api == "anthropic-messages")
            {
                model
                    .metadata
                    .compat
                    .supports_strict_tools
                    .get_or_insert(true);
            }
        }
    }
    models.insert(
        "amazon-bedrock".into(),
        serde_json::from_str(include_str!("../assets/bedrock-models.json"))
            .expect("bundled Pi Bedrock models"),
    );
    models
});

fn configure_cloudflare_workers(
    providers: &mut serde_json::Map<String, Value>,
    account: Option<&str>,
) -> Result<()> {
    const PREFIX: &str = "https://api.cloudflare.com/client/v4/accounts/";
    let account = account.filter(|id| super::operation_models::valid_cloudflare_account(id));
    let name = providers
        .keys()
        .find(|name| name.eq_ignore_ascii_case("cloudflare-workers-ai"))
        .cloned();
    if name.is_none() && account.is_none() {
        return Ok(());
    }
    let entry = providers
        .entry(name.unwrap_or_else(|| "cloudflare-workers-ai".into()))
        .or_insert_with(|| serde_json::json!({}));
    let entry = entry
        .as_object_mut()
        .ok_or_else(|| Error::Config("invalid Cloudflare Workers AI profile".into()))?;
    let account_from_base = |base: &str| {
        let (configured, route) = base
            .trim_end_matches('/')
            .strip_prefix(PREFIX)?
            .split_once('/')?;
        if !matches!(route, "ai" | "ai/run" | "ai/v1" | "ai/v1/chat/completions") {
            return None;
        }
        if configured == "{CLOUDFLARE_ACCOUNT_ID}" {
            return account.map(str::to_owned);
        }
        super::operation_models::valid_cloudflare_account(configured).then(|| configured.to_owned())
    };
    let configured_base = entry.get("baseUrl").filter(|value| !value.is_null());
    let route_account = match configured_base {
        None => account.map(str::to_owned),
        Some(value) => value.as_str().and_then(account_from_base),
    };
    let classifier_api =
        entry.get("api").and_then(Value::as_str) == Some("cloudflare-workers-ai-system-one");
    let classifier_models_own_credentials = configured_base.is_none()
        && entry.get("api").is_none_or(Value::is_null)
        && entry
            .get("models")
            .and_then(Value::as_array)
            .filter(|models| models.iter().all(|model| model["type"] == "classifier"))
            .and_then(|models| super::operation_models::cloudflare_model_binding(models))
            .is_some_and(|base| {
                let bound_account = base.as_deref().and_then(account_from_base);
                bound_account.is_none() || bound_account != route_account
            });
    let classifier_profile = classifier_api || classifier_models_own_credentials;
    let route_account =
        route_account.or_else(|| account.filter(|_| classifier_profile).map(str::to_owned));
    if classifier_profile {
        // Provider-level operations settings were consumed by the operation catalogue.
        for field in ["api", "baseUrl", "apiKey", "authHeader", "headers"] {
            entry.remove(field);
        }
    }
    if let Some(account) = route_account {
        let base = format!("{PREFIX}{account}/ai/v1");
        entry.insert("baseUrl".into(), Value::String(base));
    }
    Ok(())
}

fn fixed_cloudflare_base(base: &str) -> bool {
    !base.contains("{CLOUDFLARE_ACCOUNT_ID}") && !base.contains("{CLOUDFLARE_GATEWAY_ID}")
}

pub(super) fn cloudflare_workers_chat_account(endpoint: &str) -> Option<&str> {
    let path = endpoint.strip_prefix("https://api.cloudflare.com/client/v4/accounts/")?;
    let account = path.strip_suffix("/ai/v1/chat/completions")?;
    super::operation_models::valid_cloudflare_account(account).then_some(account)
}

impl Profiles {
    pub fn load(keys: &super::model_auth::Keys) -> Result<Self> {
        let path = config::agent_dir().join("models.json");
        let data = config::read_object(&path)?;
        let account = std::env::var("CLOUDFLARE_ACCOUNT_ID").ok();
        let mut profiles = Self::parse_with_cloudflare_routes(
            &data,
            keys.cloudflare_chat_account().or(account.as_deref()),
            keys.cloudflare_gateway_route(),
        )?;
        profiles.cloudflare_saved_account = keys.cloudflare_chat_account().map(str::to_owned);
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
                profile.provider_endpoints.remove(&profile.endpoint);
                profile.endpoint.clone_from(&endpoint);
                profile.provider_endpoints.insert(endpoint.clone());
            }
            profiles.radius_endpoint = Some(endpoint);
        }
        Ok(profiles)
    }

    pub(super) fn cloudflare_saved_account(&self) -> Option<&str> {
        self.cloudflare_saved_account.as_deref()
    }

    #[cfg(test)]
    pub(super) fn parse(data: &Value) -> Result<Self> {
        Self::parse_with_cloudflare_routes(data, None, None)
    }

    #[cfg(test)]
    fn parse_with_cloudflare_account(data: &Value, account: Option<&str>) -> Result<Self> {
        Self::parse_with_cloudflare_routes(data, account, None)
    }

    pub(super) fn cloudflare_configured_base(
        &self,
        provider: &Provider,
        model: Option<&str>,
    ) -> Option<&str> {
        let profile = self.entries.get(provider.id())?;
        model
            .and_then(|id| profile.models.iter().find(|model| model.id == id))
            .and_then(|model| model.base_url.as_deref())
            .or(profile.configured_cloudflare_base.as_deref())
    }

    pub(super) fn has_fixed_cloudflare_endpoint(
        &self,
        provider: &Provider,
        model: Option<&str>,
    ) -> bool {
        self.cloudflare_configured_base(provider, model)
            .is_some_and(fixed_cloudflare_base)
    }

    pub(super) fn cloudflare_gateway_route(&self) -> Option<&super::cloudflare_gateway::Route> {
        self.cloudflare_gateway_route.as_ref()
    }

    pub(super) fn parse_with_cloudflare_routes(
        data: &Value,
        account: Option<&str>,
        cloudflare_gateway_route: Option<super::cloudflare_gateway::Route>,
    ) -> Result<Self> {
        let empty = serde_json::Map::new();
        let providers = match data.get("providers") {
            Some(providers) => providers
                .as_object()
                .ok_or_else(|| Error::Config("models.json providers must be an object".into()))?,
            None => &empty,
        };
        let configured_providers = providers;
        let operations = super::operation_models::Catalogue::parse(configured_providers)?;
        let mut providers = configured_providers.clone();
        configure_cloudflare_workers(&mut providers, account)?;
        if let Some(route) = &cloudflare_gateway_route {
            let name = providers
                .keys()
                .find(|name| name.eq_ignore_ascii_case("cloudflare-ai-gateway"))
                .cloned();
            let entry = providers
                .entry(name.unwrap_or_else(|| "cloudflare-ai-gateway".into()))
                .or_insert_with(|| serde_json::json!({}));
            let entry = entry
                .as_object_mut()
                .ok_or_else(|| Error::Config("invalid Cloudflare AI Gateway profile".into()))?;
            let base = entry.get("baseUrl").and_then(Value::as_str);
            let routed = match base {
                None => Some(route.clone()),
                Some(base) => super::cloudflare_gateway::Route::from_configured(base, Some(route)),
            };
            if let Some(route) = routed {
                entry.insert(
                    "baseUrl".into(),
                    Value::String(
                        route
                            .endpoint(Adapter::ChatCompletions)
                            .expect("supported gateway API"),
                    ),
                );
            }
        }

        let mut entries = BTreeMap::new();
        for (name, entry) in &providers {
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
            let mut provider_endpoints = BTreeSet::from([endpoint.clone()]);
            profile
                .models
                .retain(|model| model.kind.as_deref().is_none_or(|kind| kind == "chat"));
            let mut ids = std::collections::BTreeSet::new();
            let mut model_endpoints = BTreeMap::new();
            for model in &mut profile.models {
                // Pi defaults definitions to text; overrides remain partial.
                model
                    .metadata
                    .input
                    .get_or_insert_with(|| vec![Modality::Text]);
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
                if let Some(base) = profile.base_url.as_deref() {
                    provider_endpoints.insert(endpoint_from_base(&provider, adapter, base)?);
                } else if let Some(official) = builtin_chat::find(provider.id())
                    .and_then(|builtin| builtin.endpoint_for(adapter))
                {
                    provider_endpoints.insert(official.to_owned());
                }
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
                    provider_endpoints.insert(endpoint.clone());
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
            let configured_cloudflare_base = configured_providers
                .get(name)
                .filter(|configured| {
                    matches!(
                        provider.id(),
                        "cloudflare-workers-ai" | "cloudflare-ai-gateway"
                    ) && configured["api"] != "cloudflare-workers-ai-system-one"
                })
                .and_then(|configured| configured["baseUrl"].as_str())
                .map(str::to_owned);
            let id = provider.id().to_owned();
            if entries
                .insert(
                    id.clone(),
                    Profile {
                        provider,
                        endpoint,
                        base_url: profile.base_url,
                        configured_cloudflare_base,
                        provider_endpoints,
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
        let mut revision = Sha256::new();
        revision.update(serde_json::to_vec(data)?);
        if &providers != configured_providers {
            revision.update(serde_json::to_vec(&providers)?);
        }
        let revision = format!("{:x}", revision.finalize());
        Ok(Self {
            entries,
            revision,
            cloudflare_saved_account: None,
            cloudflare_gateway_route,
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
                self.provider("openai-chatgpt").expect("ChatGPT provider"),
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
                            && profile.provider.id() != "openai-chatgpt"
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
        if !profile.provider_endpoints.contains(endpoint) {
            return None;
        }
        Credential::configured(profile.api_key.as_deref()?)
    }

    pub fn has_provider_endpoint(&self, provider: &Provider, endpoint: &str) -> bool {
        self.entries
            .get(provider.id())
            .is_some_and(|profile| profile.provider_endpoints.contains(endpoint))
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

    /// Explicit settings only; bundled chat defaults do not apply to Google.
    pub fn google_strict_tool_policy(
        &self,
        provider: &Provider,
    ) -> (Option<bool>, BTreeMap<String, bool>) {
        let Some(profile) = self.entries.get(provider.id()) else {
            return (None, BTreeMap::new());
        };
        let default = profile.compat.chat.supports_strict_mode();
        let mut models = BTreeMap::new();
        for model in &profile.models {
            if let Some(value) = model.metadata.compat.chat.supports_strict_mode() {
                models.insert(model.id.clone(), value);
            }
        }
        for (id, model) in &profile.overrides {
            if let Some(value) = model.compat.chat.supports_strict_mode() {
                models.insert(id.clone(), value);
            }
        }
        (default, models)
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

    pub(super) fn responses_strict_tool_support(
        &self,
        provider: &Provider,
        model: Option<&ModelMetadata>,
    ) -> bool {
        self.chat_compatibility(provider, model)
            .supports_strict_mode()
            .unwrap_or_else(|| matches!(provider.id(), "openai-chatgpt" | "openai-codex"))
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

    pub fn bedrock_long_cache_override(
        &self,
        provider: &Provider,
        model: Option<&ModelMetadata>,
    ) -> Option<bool> {
        self.cache_compatibility(provider, model)
            .supports_long_cache_retention
    }

    pub fn anthropic_strict_tool_support(
        &self,
        provider: &Provider,
        model: Option<&ModelMetadata>,
    ) -> bool {
        self.cache_compatibility(provider, model)
            .supports_strict_tools
            .unwrap_or(false)
    }

    /// Freeze known Anthropic model capabilities sharing the selected route.
    pub fn anthropic_strict_tool_models(
        &self,
        provider: &Provider,
        selected: &str,
        explicit_endpoint: Option<&str>,
    ) -> BTreeMap<String, bool> {
        let endpoint = explicit_endpoint
            .or_else(|| self.model_endpoint(provider, selected))
            .unwrap_or_else(|| self.endpoint(provider));
        // A partial model override does not establish a routable model.
        let ids = self.model_ids(provider);
        let mut models: BTreeMap<String, bool> = ids
            .into_iter()
            .filter_map(|id| {
                if self.model_adapter(provider, &id) != Adapter::Anthropic
                    || explicit_endpoint
                        .or_else(|| self.model_endpoint(provider, &id))
                        .unwrap_or_else(|| self.endpoint(provider))
                        != endpoint
                {
                    return None;
                }
                let metadata = self.model(provider, &id)?;
                let supported = self.anthropic_strict_tool_support(provider, Some(&metadata));
                Some((id, supported))
            })
            .collect();
        // The selected model may be absent from the catalog; its provider-level
        // capability (or the default false) still applies to that selection.
        models.entry(selected.to_owned()).or_insert_with(|| {
            let metadata = self.model(provider, selected);
            self.anthropic_strict_tool_support(provider, metadata.as_ref())
        });
        models
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

    pub fn responses_max_output_tokens(
        &self,
        provider: &Provider,
        model: Option<&ModelMetadata>,
    ) -> bool {
        self.cache_compatibility(provider, model)
            .supports_max_output_tokens
            .unwrap_or(true)
    }

    pub fn responses_cache_support(
        &self,
        provider: &Provider,
        model: Option<&ModelMetadata>,
    ) -> (bool, bool) {
        let compat = self.cache_compatibility(provider, model);
        (
            compat.supports_long_cache_retention.unwrap_or(true),
            compat.supports_explicit_prompt_cache_mode.unwrap_or(false),
        )
    }

    pub fn responses_prompt_cache_key_enabled(
        &self,
        provider: &Provider,
        model: Option<&ModelMetadata>,
    ) -> bool {
        !matches!(
            self.cache_compatibility(provider, model)
                .prompt_cache_key_mode,
            Some(PromptCacheKeyMode::Disabled)
        )
    }

    pub fn chat_prompt_cache_policy(
        &self,
        provider: &Provider,
        model_id: &str,
        model: Option<&ModelMetadata>,
        endpoint: &str,
        long_requested: bool,
    ) -> (bool, bool) {
        let compat = self.cache_compatibility(provider, model);
        let raw = self
            .entries
            .get(provider.id())
            .and_then(|profile| {
                let selected = profile
                    .model_endpoints
                    .get(model_id)
                    .map_or(profile.endpoint.as_str(), String::as_str);
                if selected != endpoint {
                    return None;
                }
                profile
                    .models
                    .iter()
                    .find(|item| item.id == model_id)
                    .and_then(|item| item.base_url.as_deref())
                    .or(profile.base_url.as_deref())
            })
            .unwrap_or(endpoint);
        let long_supported = compat.supports_long_cache_retention.unwrap_or_else(|| {
            provider != &Provider::Ollama
                && !matches!(
                    provider.id(),
                    "together"
                        | "cloudflare-workers-ai"
                        | "cloudflare-ai-gateway"
                        | "nvidia"
                        | "ant-ling"
                )
                && ![
                    "api.together.ai",
                    "api.together.xyz",
                    "api.cloudflare.com",
                    "gateway.ai.cloudflare.com",
                    "integrate.api.nvidia.com",
                    "api.ant-ling.com",
                ]
                .iter()
                .any(|needle| raw.contains(needle))
        });
        let long = long_requested && long_supported;
        let key_enabled = match compat
            .prompt_cache_key_mode
            .unwrap_or(PromptCacheKeyMode::Auto)
        {
            PromptCacheKeyMode::Disabled => false,
            PromptCacheKeyMode::Enabled => true,
            PromptCacheKeyMode::Auto => raw.contains("api.openai.com") || long,
        };
        (key_enabled, long)
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
        if adapter == Adapter::MistralConversations {
            return &["x-affinity"];
        }
        let opencode = matches!(provider.id(), "opencode" | "opencode-go");
        let routing: &'static [&'static str] = if opencode {
            &["x-opencode-session"]
        } else {
            &[]
        };
        if !matches!(
            adapter,
            Adapter::Anthropic
                | Adapter::ChatCompletions
                | Adapter::OpenAiResponses
                | Adapter::CodexResponses
        ) || provider.id() == "openai-chatgpt"
        {
            return routing;
        }
        let profile = self
            .entries
            .get(provider.id())
            .map(|profile| &profile.compat);
        let metadata = self.model(provider, model);
        let model_compat = metadata.as_ref().map(|model| &model.compat);
        let openrouter = provider.id() == "openrouter"
            || if adapter == Adapter::OpenAiResponses {
                let raw = self.entries.get(provider.id()).and_then(|profile| {
                    let selected = profile
                        .model_endpoints
                        .get(model)
                        .map_or(profile.endpoint.as_str(), String::as_str);
                    if selected != endpoint {
                        return None;
                    }
                    profile
                        .models
                        .iter()
                        .find(|item| item.id == model)
                        .and_then(|item| item.base_url.as_deref())
                        .or(profile.base_url.as_deref())
                });
                raw.unwrap_or(endpoint).contains("openrouter.ai")
            } else {
                reqwest::Url::parse(endpoint)
                    .ok()
                    .is_some_and(|url| url.host_str() == Some("openrouter.ai"))
            };
        let send = model_compat
            .and_then(|compat| compat.send_session_affinity_headers)
            .or_else(|| profile.and_then(|compat| compat.send_session_affinity_headers))
            .unwrap_or(
                openrouter || matches!(adapter, Adapter::OpenAiResponses | Adapter::CodexResponses),
            );
        if !send {
            return routing;
        }
        let format = model_compat
            .and_then(|compat| compat.session_affinity_format)
            .or_else(|| profile.and_then(|compat| compat.session_affinity_format))
            .unwrap_or(if openrouter {
                AffinityFormat::Openrouter
            } else {
                AffinityFormat::Openai
            });
        match (adapter, format) {
            (_, AffinityFormat::Openrouter) if opencode => &["x-opencode-session", "x-session-id"],
            (Adapter::OpenAiResponses, AffinityFormat::Openai) if opencode => {
                &["x-opencode-session", "session-id", "x-client-request-id"]
            }
            (Adapter::OpenAiResponses, AffinityFormat::OpenaiNosession) if opencode => {
                &["x-opencode-session", "x-client-request-id"]
            }
            (Adapter::Anthropic, _) if opencode => &["x-opencode-session", "x-session-affinity"],
            (_, AffinityFormat::Openai) if opencode => &[
                "x-opencode-session",
                "session_id",
                "x-client-request-id",
                "x-session-affinity",
            ],
            (_, AffinityFormat::OpenaiNosession) if opencode => &[
                "x-opencode-session",
                "x-client-request-id",
                "x-session-affinity",
            ],
            (_, AffinityFormat::Openrouter) => &["x-session-id"],
            (Adapter::OpenAiResponses | Adapter::CodexResponses, AffinityFormat::Openai) => {
                &["session-id", "x-client-request-id"]
            }
            (
                Adapter::OpenAiResponses | Adapter::CodexResponses,
                AffinityFormat::OpenaiNosession,
            ) => &["x-client-request-id"],
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

    pub(super) fn has_model_definition(&self, provider: &Provider, id: &str) -> bool {
        self.definition(provider, id).is_some()
    }

    pub fn model_adapter(&self, provider: &Provider, model: &str) -> Adapter {
        self.definition(provider, model)
            .and_then(|model| model.api.as_deref())
            .and_then(Adapter::named)
            .unwrap_or_else(|| provider.adapter())
    }

    pub fn model(&self, provider: &Provider, id: &str) -> Option<ModelMetadata> {
        let profile = self.entries.get(provider.id());
        let mut model = self
            .definition(provider, id)
            .map(|model| model.metadata.clone().with_model_defaults());
        // Pi applies provider compatibility over bundled model defaults, while
        // explicitly configured models and modelOverrides take precedence.
        if let (Some(profile), Some(model)) = (profile, &mut model)
            && !profile.models.iter().any(|configured| configured.id == id)
        {
            model.compat = model.compat.clone().overlay(profile.compat.clone());
        }
        match profile.and_then(|profile| profile.overrides.get(id)) {
            Some(overrides) => Some(model.unwrap_or_default().overlay(overrides)),
            None => model,
        }
    }
}

fn endpoint_from_base(provider: &Provider, adapter: Adapter, base: &str) -> Result<String> {
    let invalid = || Error::Config(format!("invalid models.json baseUrl for {}", provider.id()));
    if provider.id() == "cloudflare-ai-gateway"
        && let Some(endpoint) = super::cloudflare_gateway::Route::from_endpoint(base)
            .and_then(|route| route.endpoint(adapter))
    {
        return Ok(endpoint);
    }

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
        Adapter::CodexResponses => {
            let path = url.path().trim_end_matches('/');
            let normalized = if path.ends_with("/codex/responses") || path.ends_with("/responses") {
                path.to_owned()
            } else if path.ends_with("/codex") {
                format!("{path}/responses")
            } else {
                format!("{path}/codex/responses")
            };
            url.set_path(&normalized);
            return Ok(url.to_string());
        }
        Adapter::ChatCompletions | Adapter::MistralConversations => "/chat/completions",
        Adapter::OpenAiResponses => "/responses",
        Adapter::Anthropic | Adapter::PiMessages => "/messages",
        Adapter::Bedrock => return Ok(url.to_string()),
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
    } else if adapter == Adapter::Anthropic
        && url.host_str() == Some("opencode.ai")
        && matches!(
            (provider.id(), path),
            ("opencode", "/inference/anthropic") | ("opencode-go", "/inference/go/anthropic")
        )
    {
        format!("{path}/v1{suffix}")
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

pub(super) fn expand_value_with_env(
    value: &str,
    env: Option<&serde_json::Map<String, Value>>,
) -> Option<String> {
    pi_rs_workbench::auth_factory::expand_config_value(value, usize::MAX, |name| {
        env.and_then(|env| env.get(name))
            .and_then(Value::as_str)
            .filter(|value| !value.is_empty())
            .map(str::to_owned)
            .or_else(|| std::env::var(name).ok())
    })
    .ok()
    .flatten()
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
    fn cloudflare_worker_chats_and_classifiers_keep_saved_account_scope() {
        let keys = super::super::model_auth::Keys::parse(&json!({
            "cloudflare-workers-ai":{
                "type":"api_key", "key":"fixture","env":{"CLOUDFLARE_ACCOUNT_ID":"mine"}
            }
        }));
        let profiles =
            Profiles::parse_with_cloudflare_account(&json!({}), keys.cloudflare_chat_account())
                .unwrap();
        let provider = profiles.provider("cloudflare-workers-ai").unwrap();
        let endpoint = profiles.endpoint(&provider);
        assert_eq!(
            endpoint,
            "https://api.cloudflare.com/client/v4/accounts/mine/ai/v1/chat/completions"
        );
        assert_eq!(profiles.model_ids(&provider).len(), 19);
        assert_eq!(profiles.model_ids(&provider)[0], "@cf/moonshotai/kimi-k2.6");
        assert_eq!(
            keys.custom_credential(&provider, endpoint, &profiles)
                .and_then(|key| key.literal()),
            Some("fixture")
        );
        assert!(
            keys.operation_credential(
                "cloudflare-workers-ai",
                "https://api.cloudflare.com/client/v4/accounts/mine/ai/run",
                true
            )
            .is_some()
        );
        assert!(
            keys.operation_credential(
                "cloudflare-workers-ai",
                "https://api.cloudflare.com/client/v4/accounts/other/ai/run",
                true
            )
            .is_some()
        );

        let other = Profiles::parse(&json!({"providers":{"cloudflare-workers-ai":{
            "baseUrl":"https://api.cloudflare.com/client/v4/accounts/other/ai/v1"
        }}}))
        .unwrap();
        assert!(
            keys.custom_credential(&provider, other.endpoint(&provider), &other)
                .is_none()
        );
        assert!(
            keys.environment(&provider, other.endpoint(&provider), &other)
                .is_none()
        );
    }

    #[test]
    fn cloudflare_classifier_only_api_does_not_override_chat_api() {
        let profiles = Profiles::parse(&json!({"providers":{"cloudflare-workers-ai":{
            "baseUrl":"https://api.cloudflare.com/client/v4/accounts/mine/ai",
            "api":"cloudflare-workers-ai-system-one","apiKey":"classifier-only",
            "models":[{"id":"typesafe/jev","type":"classifier"}]
        }}}))
        .unwrap();
        let provider = profiles.provider("cloudflare-workers-ai").unwrap();
        let endpoint = profiles.endpoint(&provider);
        assert_eq!(
            endpoint,
            "https://api.cloudflare.com/client/v4/accounts/mine/ai/v1/chat/completions"
        );
        assert!(profiles.credential(&provider, endpoint).is_none());
        assert_eq!(profiles.model_ids(&provider).len(), 19);
    }

    #[test]
    fn cloudflare_chat_models_survive_a_custom_classifier_route() {
        let profiles = Profiles::parse_with_cloudflare_account(
            &json!({"providers":{"cloudflare-workers-ai":{
                "baseUrl":"https://classifiers.example/v1",
                "api":"cloudflare-workers-ai-system-one","apiKey":"classifier-only",
                "models":[
                    {"id":"typesafe/jev","type":"classifier"},
                    {"id":"custom-chat","api":"openai-completions","name":"Custom chat"}
                ]
            }}}),
            Some("mine"),
        )
        .unwrap();
        let provider = profiles.provider("cloudflare-workers-ai").unwrap();
        let endpoint = profiles.endpoint(&provider);
        assert_eq!(
            endpoint,
            "https://api.cloudflare.com/client/v4/accounts/mine/ai/v1/chat/completions"
        );
        assert!(profiles.credential(&provider, endpoint).is_none());
        assert!(
            profiles
                .model_ids(&provider)
                .contains(&"custom-chat".to_owned())
        );
    }

    #[test]
    fn inferred_classifier_credentials_do_not_apply_to_official_chat() {
        let profiles = Profiles::parse_with_cloudflare_account(
            &json!({"providers":{"cloudflare-workers-ai":{
                "api":null,"baseUrl":null,
                "apiKey":"classified","headers":{"x-provider-secret":"value"},
                "models":[{"id":"custom","type":"classifier",
                    "api":"cloudflare-workers-ai-system-one",
                    "baseUrl":"https://classifiers.example/v1"}]
            }}}),
            Some("mine"),
        )
        .unwrap();
        let provider = profiles.provider("cloudflare-workers-ai").unwrap();
        let endpoint = profiles.endpoint(&provider);
        assert_eq!(cloudflare_workers_chat_account(endpoint), Some("mine"));
        assert!(profiles.credential(&provider, endpoint).is_none());
        let headers = profiles
            .header_settings(&provider, endpoint, None)
            .static_headers(&provider, None)
            .unwrap();
        assert!(!headers.contains_key("x-provider-secret"));
    }

    #[test]
    fn matching_classifier_routes_keep_explicit_provider_chat_credentials() {
        for base in [
            Value::Null,
            json!("https://api.cloudflare.com/client/v4/accounts/mine/ai/run"),
            json!("https://api.cloudflare.com/client/v4/accounts/{CLOUDFLARE_ACCOUNT_ID}/ai"),
        ] {
            let profiles = Profiles::parse_with_cloudflare_account(
                &json!({"providers":{"cloudflare-workers-ai":{
                    "apiKey":"fixture","headers":{"x-provider-setting":"value"},
                    "models":[{"id":"custom","type":"classifier",
                        "api":"cloudflare-workers-ai-system-one","baseUrl":base}]
                }}}),
                Some("mine"),
            )
            .unwrap();
            let provider = profiles.provider("cloudflare-workers-ai").unwrap();
            let endpoint = profiles.endpoint(&provider);
            assert_eq!(
                profiles
                    .credential(&provider, endpoint)
                    .as_ref()
                    .and_then(|key| key.literal()),
                Some("fixture"),
            );
            let headers = profiles
                .header_settings(&provider, endpoint, None)
                .static_headers(&provider, None)
                .unwrap();
            assert_eq!(headers["x-provider-setting"], "value");
        }
    }

    #[test]
    fn implicit_cloudflare_keys_are_available_only_on_official_chat_routes() {
        let keys = super::super::model_auth::Keys::parse(&json!({
            "cloudflare-workers-ai":{"type":"api_key","key":"fixture"}
        }));
        for (base, expected) in [
            ("https://example.org/client/v4/accounts/mine/ai/v1", false),
            (
                "https://api.cloudflare.com/client/v4/accounts/mine/ai/v1",
                true,
            ),
        ] {
            let profiles = Profiles::parse(&json!({"providers":{"cloudflare-workers-ai":{
                "baseUrl":base
            }}}))
            .unwrap();
            let provider = profiles.provider("cloudflare-workers-ai").unwrap();
            let endpoint = profiles.endpoint(&provider);
            assert_eq!(
                keys.custom_credential(&provider, endpoint, &profiles)
                    .is_some(),
                expected
            );
        }
    }

    #[test]
    fn chat_scoped_cloudflare_key_can_classify_in_same_account() {
        let keys = super::super::model_auth::Keys::parse(&json!({
            "cloudflare-workers-ai":{
                "type":"api_key", "key":"fixture",
                "endpoint":"https://api.cloudflare.com/client/v4/accounts/mine/ai/v1/chat/completions"
            }
        }));
        assert_eq!(keys.cloudflare_operation_account(), Some("mine"));
        assert_eq!(keys.cloudflare_chat_account(), Some("mine"));
        for (host, account, official, available) in [
            ("api.cloudflare.com", "mine", true, true),
            ("api.cloudflare.com", "other", true, false),
            ("example.org", "mine", false, false),
        ] {
            assert_eq!(
                keys.operation_credential(
                    "cloudflare-workers-ai",
                    &format!("https://{host}/client/v4/accounts/{account}/ai/run"),
                    official,
                )
                .is_some(),
                available
            );
        }
    }

    #[test]
    fn cloudflare_worker_chats_normalize_official_classifier_base_urls() {
        let provider = Provider::named("cloudflare-workers-ai").unwrap();
        for (base, account) in [
            (
                "https://api.cloudflare.com/client/v4/accounts/mine/ai/run/",
                None,
            ),
            (
                "https://api.cloudflare.com/client/v4/accounts/{CLOUDFLARE_ACCOUNT_ID}/ai",
                Some("mine"),
            ),
        ] {
            let profiles = Profiles::parse_with_cloudflare_account(
                &json!({"providers":{"cloudflare-workers-ai":{"baseUrl":base}}}),
                account,
            )
            .unwrap();
            assert_eq!(
                profiles.endpoint(&provider),
                "https://api.cloudflare.com/client/v4/accounts/mine/ai/v1/chat/completions"
            );
        }
    }

    #[test]
    fn anthropic_explicit_endpoint_overrides_model_endpoints() {
        let profiles = Profiles::parse(&json!({"providers":{"proxy":{
            "api":"anthropic-messages", "baseUrl":"http://localhost:1/v1",
            "models":[
                {"id":"a", "baseUrl":"http://localhost:1/v1", "compat":{"supportsStrictTools":true}},
                {"id":"b", "baseUrl":"http://localhost:2/v1", "compat":{"supportsStrictTools":false}},
                {"id":"other-api", "api":"openai-completions", "compat":{"supportsStrictTools":true}}
            ]
        }}})).unwrap();
        let provider = profiles.provider("proxy").unwrap();
        for (selected, rewritten, expected) in [("a", "b", false), ("b", "a", true)] {
            let normal = profiles.anthropic_strict_tool_models(&provider, selected, None);
            assert!(!normal.contains_key(rewritten));
            let explicit = profiles.anthropic_strict_tool_models(
                &provider,
                selected,
                Some("http://localhost:3/v1"),
            );
            assert_eq!(explicit.get(rewritten), Some(&expected));
            assert!(!explicit.contains_key("other-api"));
        }
    }

    #[test]
    fn anthropic_selected_uncatalogued_model_keeps_provider_capability() {
        for configured in [None, Some(false), Some(true)] {
            let profiles = Profiles::parse(&json!({"providers":{"proxy":{
                "api":"anthropic-messages", "baseUrl":"http://127.0.0.1:1234/v1", "compat":{"supportsStrictTools":configured},
                "models":[{"id":"known","compat":{"supportsStrictTools":true}}],
                "modelOverrides":{"rewritten-unknown":{"name":"partial override"},"selected-overridden":{"compat":{"supportsStrictTools":false}}}
            }}})).unwrap();
            let provider = profiles.provider("proxy").unwrap();
            let models = profiles.anthropic_strict_tool_models(&provider, "selected-unknown", None);
            assert_eq!(
                models.get("selected-unknown"),
                Some(&configured.unwrap_or(false))
            );
            assert_eq!(models.get("known"), Some(&true));
            assert!(!models.contains_key("rewritten-unknown"));
            assert!(!models.contains_key("selected-overridden"));
            let selected =
                profiles.anthropic_strict_tool_models(&provider, "selected-overridden", None);
            assert_eq!(selected.get("selected-overridden"), Some(&false));
        }
    }

    #[test]
    fn anthropic_strict_tool_profile_overlays_and_catalog_defaults() {
        let default = Profiles::default();
        let anthropic = Provider::Anthropic;
        let id = default.model_ids(&anthropic).into_iter().next().unwrap();
        assert!(
            default
                .anthropic_strict_tool_support(&anthropic, default.model(&anthropic, &id).as_ref())
        );
        assert!(!default.anthropic_strict_tool_support(
            &anthropic,
            default.model(&anthropic, "unknown").as_ref()
        ));
        let config = json!({"providers":{
            "anthropic":{"compat":{"supportsStrictTools":false}, "modelOverrides":{(id.clone()):{"compat":{"supportsStrictTools":true}}}},
            "proxy":{"api":"anthropic-messages","baseUrl":"https://example.com", "compat":{"supportsStrictTools":true}, "models":[
                {"id":"inherit"},{"id":"disabled","compat":{"supportsStrictTools":false}},{"id":"enabled","compat":{"supportsStrictTools":true}}],
                "modelOverrides":{"inherit":{"compat":{"supportsStrictTools":false}}}},
            "other":{"api":"anthropic-messages","baseUrl":"https://example.com", "models":[{"id":"plain"}]}
        }});
        let profiles = Profiles::parse(&config).unwrap();
        let other_builtin = default
            .model_ids(&anthropic)
            .into_iter()
            .find(|name| name != &id)
            .unwrap();
        assert!(!profiles.anthropic_strict_tool_support(
            &anthropic,
            profiles.model(&anthropic, &other_builtin).as_ref()
        ));
        assert!(
            profiles.anthropic_strict_tool_support(
                &anthropic,
                profiles.model(&anthropic, &id).as_ref()
            )
        );
        let proxy = profiles.provider("proxy").unwrap();
        for (id, expected) in [
            ("inherit", false),
            ("disabled", false),
            ("enabled", true),
            ("unknown", true),
        ] {
            assert_eq!(
                profiles.anthropic_strict_tool_support(&proxy, profiles.model(&proxy, id).as_ref()),
                expected,
                "{id}"
            );
        }
        let other = profiles.provider("other").unwrap();
        assert!(
            !profiles
                .anthropic_strict_tool_support(&other, profiles.model(&other, "plain").as_ref())
        );
    }

    #[test]
    fn bedrock_application_profile_retains_configured_model_name() {
        let id = "arn:aws:bedrock:us-east-1:123456789012:application-inference-profile/opaque";
        let profiles = Profiles::parse(
            &json!({"providers":{"amazon-bedrock":{"models":[{"id":id,"name":"Claude Sonnet"}]}}}),
        )
        .unwrap();
        let provider = profiles.provider("amazon-bedrock").unwrap();
        assert_eq!(
            profiles.model(&provider, id).unwrap().name.as_deref(),
            Some("Claude Sonnet")
        );
    }

    #[test]
    fn bedrock_catalog_exposes_pi_models_and_metadata_offline() {
        let profiles = Profiles::parse(&json!({})).unwrap();
        let provider = profiles.provider("amazon-bedrock").unwrap();
        assert_eq!(provider.adapter(), Adapter::Bedrock);
        assert!(
            profiles
                .model_ids(&provider)
                .contains(&"amazon.nova-lite-v1:0".to_owned())
        );
        let model = profiles.model(&provider, "amazon.nova-lite-v1:0").unwrap();
        assert_eq!(model.context_window, Some(300_000));
        assert_eq!(model.max_tokens, Some(10_000));
    }

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
    fn meta_catalog_and_credential_are_endpoint_bound() {
        const CHILD: &str = "PI_RS_TEST_META_AUTH_CHILD_932B";
        if std::env::var_os(CHILD).is_none() {
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "model_profiles::tests::meta_catalog_and_credential_are_endpoint_bound",
                ])
                .env(CHILD, "1")
                .env("META_API_KEY", "meta-environment-key")
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "{}\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed"));
            return;
        }
        let mut args = super::super::Cli::parse_from(["pi"]);
        args.model_profiles = Profiles::parse(&json!({})).unwrap();
        let provider = args.model_profiles.provider("meta").unwrap();
        assert_eq!(provider.adapter(), Adapter::OpenAiResponses);
        let endpoint = args.model_profiles.endpoint(&provider);
        assert_eq!(endpoint, "https://api.meta.ai/v1/responses");
        assert_eq!(args.model_profiles.model_ids(&provider).len(), 6);
        let model = args
            .model_profiles
            .model(&provider, "muse-spark-1.3")
            .unwrap();
        assert_eq!(model.context_window, Some(1_048_576));
        assert_eq!(model.max_tokens, Some(131_072));
        assert_eq!(model.supports_images(), Some(true));
        assert_eq!(model.mapped_effort("max"), Some(Some("max")));
        assert_eq!(model.image_resize().unwrap().max_width, Some(2000));
        let mut metadata = json!({});
        model.apply_catalog_metadata(&mut metadata);
        assert_eq!(metadata["cost"]["input"], 1.25);
        assert_eq!(metadata["cost"]["cacheRead"], 0.15);
        let old = args
            .model_profiles
            .model(&provider, "muse-spark-1.2")
            .unwrap();
        assert_eq!(old.mapped_effort("max"), Some(None));
        assert_eq!(
            args.configured_model_credential(&provider, endpoint)
                .unwrap()
                .literal(),
            Some("meta-environment-key")
        );
        assert!(
            args.configured_model_credential(&provider, "https://api.meta.ai:444/v1/responses")
                .is_none()
        );
        assert!(
            args.configured_model_credential(&provider, "https://other.example/v1/responses")
                .is_none()
        );
        args.model_keys = super::super::model_auth::Keys::parse(
            &json!({"meta":{"type":"api_key","key":"stored-key"}}),
        );
        assert_eq!(
            args.configured_model_credential(&provider, endpoint)
                .unwrap()
                .literal(),
            Some("stored-key")
        );
        assert!(
            args.configured_model_credential(&provider, "https://other.example/v1/responses")
                .is_none()
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
    fn chat_prompt_cache_policy_matches_modes_and_long_retention_support() {
        let profiles = Profiles::parse(&json!({"providers":{
            "test":{"api":"openai-completions", "baseUrl":"https://example.test/v1", "models":[
                {"id":"auto"}, {"id":"enabled", "compat":{"promptCacheKeyMode":"enabled"}},
                {"id":"disabled", "compat":{"promptCacheKeyMode":"disabled"}},
                {"id":"no-long", "compat":{"supportsLongCacheRetention":false}}
            ]}
        }}))
        .unwrap();
        let provider = profiles.provider("test").unwrap();
        for (id, endpoint, request_long, expected) in [
            ("auto", "https://example.test/v1", false, (false, false)),
            ("auto", "https://example.test/v1", true, (true, true)),
            ("auto", "https://api.openai.com/v1", false, (true, false)),
            ("enabled", "https://example.test/v1", false, (true, false)),
            ("disabled", "https://example.test/v1", true, (false, true)),
            ("no-long", "https://example.test/v1", true, (false, false)),
            ("auto", "https://api.together.ai/v1", true, (false, false)),
        ] {
            let model = profiles.model(&provider, id).unwrap();
            assert_eq!(
                profiles.chat_prompt_cache_policy(
                    &provider,
                    id,
                    Some(&model),
                    endpoint,
                    request_long
                ),
                expected
            );
        }
        let ollama = profiles.provider("ollama").unwrap();
        assert_eq!(
            profiles.chat_prompt_cache_policy(
                &ollama,
                "m",
                None,
                "http://localhost:11434/v1",
                true
            ),
            (false, false)
        );
    }

    #[test]
    fn responses_session_affinity_uses_api_specific_headers_and_overrides() {
        let profiles = Profiles::parse(&json!({"providers":{
            "custom":{"baseUrl":"https://example.test/v1", "api":"openai-responses", "models":[
                {"id":"default"}, {"id":"router", "compat":{"sessionAffinityFormat":"openrouter"}},
                {"id":"nosession", "compat":{"sessionAffinityFormat":"openai-nosession"}},
                {"id":"disabled", "compat":{"sendSessionAffinityHeaders":false}}
            ]}
        }}))
        .unwrap();
        let provider = profiles.provider("custom").unwrap();
        assert_eq!(
            profiles.session_affinity(&provider, "default", "https://example.test"),
            ["session-id", "x-client-request-id"]
        );
        assert_eq!(
            profiles.session_affinity(&provider, "router", "https://example.test"),
            ["x-session-id"]
        );
        assert_eq!(
            profiles.session_affinity(&provider, "nosession", "https://example.test"),
            ["x-client-request-id"]
        );
        assert!(
            profiles
                .session_affinity(&provider, "disabled", "https://example.test")
                .is_empty()
        );
        assert_eq!(
            profiles.session_affinity(&provider, "default", "https://openrouter.ai/api/v1"),
            ["x-session-id"]
        );
        assert_eq!(
            profiles.session_affinity(&provider, "default", "https://edge.openrouter.ai/api/v1"),
            ["x-session-id"]
        );
        for (base, expected) in [
            (
                "https://OPENROUTER.AI/v1",
                &["session-id", "x-client-request-id"][..],
            ),
            (
                "https://example.com/openrouter.ai/../v1",
                &["x-session-id"][..],
            ),
        ] {
            let configured = Profiles::parse(&json!({"providers":{"proxy":{
                "api":"openai-responses", "baseUrl":base, "models":[{"id":"m"}]
            }}}))
            .unwrap();
            let selected = configured.provider("proxy").unwrap();
            assert_eq!(
                configured.session_affinity(&selected, "m", configured.endpoint(&selected)),
                expected
            );
        }
        let chatgpt = profiles.provider("openai-chatgpt").unwrap();
        assert!(
            profiles
                .session_affinity(&chatgpt, "gpt", "https://example.test")
                .is_empty()
        );
    }

    #[test]
    fn responses_prompt_cache_key_mode_inherits_and_overrides() {
        let profiles = Profiles::parse(&json!({"providers":{"example":{
            "baseUrl":"https://example.com/v1", "api":"openai-responses",
            "compat":{"promptCacheKeyMode":"disabled"},
            "models":[{"id":"inherited"},{"id":"auto","compat":{"promptCacheKeyMode":"auto"}},{"id":"enabled","compat":{"promptCacheKeyMode":"enabled"}}]
        }}})).unwrap();
        let provider = profiles.provider("example").unwrap();
        for (id, expected) in [("inherited", false), ("auto", true), ("enabled", true)] {
            let model = profiles.model(&provider, id).unwrap();
            assert_eq!(
                profiles.responses_prompt_cache_key_enabled(&provider, Some(&model)),
                expected
            );
        }
        assert!(Profiles::parse(&json!({"providers":{"example":{"baseUrl":"https://example.com/v1", "api":"openai-responses", "compat":{"promptCacheKeyMode":"unknown"}}}})).is_err());
    }

    #[test]
    fn responses_cache_compatibility_merges_provider_and_model() {
        let profiles = Profiles::parse(&json!({"providers":{"example":{
            "baseUrl":"https://example.com/v1", "api":"openai-responses",
            "compat":{"supportsLongCacheRetention":false,"supportsExplicitPromptCacheMode":true},
            "models":[{"id":"m","compat":{"supportsLongCacheRetention":true}}]
        }}}))
        .unwrap();
        let provider = profiles.provider("example").unwrap();
        let model = profiles.model(&provider, "m").unwrap();
        assert_eq!(
            profiles.responses_cache_support(&provider, Some(&model)),
            (true, true)
        );
        assert_eq!(
            profiles.responses_cache_support(&provider, None),
            (false, true)
        );
        let openai = profiles.provider("openai").unwrap();
        let model = profiles.model(&openai, "gpt-6-sol").unwrap();
        assert_eq!(
            profiles.responses_cache_support(&openai, Some(&model)),
            (true, true)
        );
    }

    #[test]
    fn builtin_provider_compatibility_precedes_catalog_but_not_overrides() {
        let profiles = Profiles::parse(&json!({"providers":{"openai":{
            "compat":{"supportsLongCacheRetention":false},
            "modelOverrides":{"gpt-6-sol":{"compat":{"supportsLongCacheRetention":true}}},
            "models":[{"id":"custom","compat":{"supportsLongCacheRetention":true}}]
        }}}))
        .unwrap();
        let provider = profiles.provider("openai").unwrap();
        let builtin = profiles.model(&provider, "gpt-6-sol").unwrap();
        assert!(
            profiles
                .responses_cache_support(&provider, Some(&builtin))
                .0
        );
        let custom = profiles.model(&provider, "custom").unwrap();
        assert!(profiles.responses_cache_support(&provider, Some(&custom)).0);

        let without_override = Profiles::parse(&json!({"providers":{"openai":{
            "compat":{"supportsLongCacheRetention":false}
        }}}))
        .unwrap();
        let provider = without_override.provider("openai").unwrap();
        let builtin = without_override.model(&provider, "gpt-6-sol").unwrap();
        assert!(
            !without_override
                .responses_cache_support(&provider, Some(&builtin))
                .0
        );
    }

    #[test]
    fn qwen_token_plan_catalogs_keep_region_and_model_capabilities() {
        let profiles = Profiles::parse(&json!({})).unwrap();
        for (name, endpoint, key, count) in [
            (
                "qwen-token-plan",
                "token-plan.maas.qwencloudapi.com",
                "QWEN_TOKEN_PLAN_TEAM_API_KEY",
                20,
            ),
            (
                "qwen-token-plan-cn",
                "token-plan.maas.qianwenaiapi.com",
                "QWEN_TOKEN_PLAN_TEAM_CN_API_KEY",
                20,
            ),
            (
                "qwen-token-plan-individual",
                "token-plan.maas.qwencloudapi.com",
                "QWEN_TOKEN_PLAN_INDIVIDUAL_API_KEY",
                13,
            ),
            (
                "qwen-token-plan-individual-cn",
                "token-plan.maas.qianwenaiapi.com",
                "QWEN_TOKEN_PLAN_INDIVIDUAL_CN_API_KEY",
                13,
            ),
        ] {
            let provider = profiles.provider(name).unwrap();
            assert_eq!(provider.adapter(), Adapter::ChatCompletions);
            assert_eq!(
                profiles.endpoint(&provider),
                format!("https://{endpoint}/compatible-mode/v1/chat/completions")
            );
            let builtin = builtin_chat::find(name).unwrap();
            assert_eq!(builtin.env(), key);
            assert_eq!(profiles.model_ids(&provider).len(), count);
            let model = profiles.model(&provider, "qwen3.8-max").unwrap();
            assert_eq!(model.supports_images(), Some(true));
            assert_eq!(
                model.levels(Adapter::ChatCompletions).unwrap(),
                ["low", "medium", "xhigh"]
            );
            assert!(profiles.model(&provider, "qwen4-max-preview").is_some());
            assert!(profiles.model(&provider, "qwen3.8-max-preview").is_none());
            assert!(profiles.model(&provider, "deepseek-v3.2").is_none());
            assert!(profiles.model(&provider, "deepseek-v4-flash").is_none());
        }
    }

    #[test]
    fn qwen_token_plan_environment_keys_are_ordered_and_scoped() {
        const CHILD: &str = "PI_RS_TEST_QWEN_TOKEN_PLAN_KEYS";
        let entries = [
            (
                "qwen-token-plan",
                "QWEN_TOKEN_PLAN_TEAM_API_KEY",
                "QWEN_TOKEN_PLAN_API_KEY",
                "international-legacy",
            ),
            (
                "qwen-token-plan-cn",
                "QWEN_TOKEN_PLAN_TEAM_CN_API_KEY",
                "QWEN_TOKEN_PLAN_CN_API_KEY",
                "china-legacy",
            ),
            (
                "qwen-token-plan-individual",
                "QWEN_TOKEN_PLAN_INDIVIDUAL_API_KEY",
                "QWEN_TOKEN_PLAN_API_KEY",
                "international-legacy",
            ),
            (
                "qwen-token-plan-individual-cn",
                "QWEN_TOKEN_PLAN_INDIVIDUAL_CN_API_KEY",
                "QWEN_TOKEN_PLAN_CN_API_KEY",
                "china-legacy",
            ),
        ];
        let Ok(scenario) = std::env::var(CHILD) else {
            for scenario in ["preferred", "fallback", "blank"] {
                let mut command = std::process::Command::new(std::env::current_exe().unwrap());
                command
                    .args(["--exact", "model_profiles::tests::qwen_token_plan_environment_keys_are_ordered_and_scoped"])
                    .env(CHILD, scenario);
                for (provider, primary, fallback, legacy) in entries {
                    command.env(fallback, legacy).env_remove(primary);
                    match scenario {
                        "preferred" => {
                            command.env(primary, provider);
                        }
                        "blank" => {
                            command.env(primary, " ");
                        }
                        _ => {}
                    }
                }
                let output = command.output().unwrap();
                assert!(
                    output.status.success(),
                    "{scenario}: {}{}",
                    String::from_utf8_lossy(&output.stdout),
                    String::from_utf8_lossy(&output.stderr)
                );
                assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed"));
            }
            return;
        };
        let mut args = super::super::Cli::parse_from(["pi"]);
        args.model_profiles = Profiles::parse(&json!({})).unwrap();
        args.model_keys = super::super::model_auth::Keys::parse(&json!({}));
        let international_old = "https://token-plan.ap-southeast-1.maas.aliyuncs.com/compatible-mode/v1/chat/completions";
        let china_old =
            "https://token-plan.cn-beijing.maas.aliyuncs.com/compatible-mode/v1/chat/completions";
        for (name, _, _, fallback) in entries {
            let provider = args.model_profiles.provider(name).unwrap();
            let official = args.model_profiles.endpoint(&provider);
            let (legacy, foreign) = if name.ends_with("-cn") {
                (china_old, international_old)
            } else {
                (international_old, china_old)
            };
            let expected = if scenario == "preferred" {
                name
            } else {
                fallback
            };
            for endpoint in [official, legacy] {
                assert_eq!(
                    args.configured_model_credential(&provider, endpoint)
                        .unwrap()
                        .literal(),
                    Some(expected)
                );
            }
            for endpoint in [
                foreign,
                "https://token-plan.maas.qwencloudapi.com:444/compatible-mode/v1/chat/completions",
                "https://relay.example/compatible-mode/v1/chat/completions",
            ] {
                assert!(
                    args.configured_model_credential(&provider, endpoint)
                        .is_none()
                );
            }
            let saved = super::super::model_auth::Keys::parse(
                &json!({(name):{"type":"api_key", "key":"old-route-key", "endpoint":legacy}}),
            );
            assert!(saved.credential(&provider, official).is_none());
            assert_eq!(
                saved.credential(&provider, legacy).unwrap().literal(),
                Some("old-route-key")
            );
        }
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
            for id in ["MiniMax-M2.7", "MiniMax-M2.7-highspeed"] {
                assert!(
                    !profiles
                        .model_ids(&provider)
                        .iter()
                        .any(|model| model == id)
                );
            }
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
    fn opencode_catalogs_bind_each_api_to_its_own_official_route() {
        let profiles = Profiles::default();
        for (id, prefix, count, default, messages, responses) in [
            (
                "opencode",
                "/inference",
                79,
                "kimi-k2.6",
                "claude-fable-5",
                "gpt-6-sol",
            ),
            (
                "opencode-go",
                "/inference/go",
                30,
                "kimi-k3",
                "claude-haiku-5-5",
                "gpt-6-luna",
            ),
        ] {
            let provider = profiles.provider(id).unwrap();
            let builtin = builtin_chat::find(id).unwrap();
            let available = profiles.model_ids(&provider);
            assert_eq!(available.len(), count);
            assert_eq!(available[0], default);
            assert_eq!(builtin.env(), "OPENCODE_API_KEY");

            let keys = super::super::model_auth::Keys::parse(
                &json!({id:{"type":"api_key","key":"fixture"}}),
            );
            for (model, adapter, suffix, headers) in [
                (
                    default,
                    Adapter::ChatCompletions,
                    "/openai/v1/chat/completions",
                    &["x-opencode-session"][..],
                ),
                (
                    messages,
                    Adapter::Anthropic,
                    "/anthropic/v1/messages",
                    &["x-opencode-session"][..],
                ),
                (
                    "gemini-4-argon",
                    Adapter::Google,
                    "/google/v1beta",
                    &["x-opencode-session"][..],
                ),
                (
                    responses,
                    Adapter::OpenAiResponses,
                    "/openai/v1/responses",
                    &["x-opencode-session", "x-client-request-id"][..],
                ),
            ] {
                let endpoint = profiles
                    .model_endpoint(&provider, model)
                    .unwrap_or_else(|| profiles.endpoint(&provider));
                assert_eq!(endpoint, format!("https://opencode.ai{prefix}{suffix}"));
                assert_eq!(profiles.model_adapter(&provider, model), adapter);
                assert_eq!(
                    profiles.session_affinity(&provider, model, endpoint),
                    headers
                );
                assert_eq!(
                    keys.credential(&provider, endpoint)
                        .and_then(|key| key.literal()),
                    Some("fixture")
                );
            }
            assert_eq!(
                keys.credential(&provider, "https://example.org/v1/responses")
                    .and_then(|key| key.literal()),
                None
            );
            let configured = Profiles::parse(&json!({"providers":{id:{"models":[{
                "id": messages, "api": "anthropic-messages",
                "baseUrl": format!("https://opencode.ai{prefix}/anthropic")
            }]}}}))
            .unwrap();
            assert_eq!(
                configured.model_endpoint(&provider, messages),
                builtin.endpoint_for(Adapter::Anthropic)
            );
        }
    }

    #[test]
    fn baseten_and_nvidia_builtin_models_keep_capabilities_and_headers() {
        let profiles = Profiles::parse(&json!({})).unwrap();
        let baseten = profiles.provider("baseten").unwrap();
        let nvidia = profiles.provider("nvidia").unwrap();
        assert_eq!(profiles.model_ids(&baseten).len(), 15);
        assert_eq!(profiles.model_ids(&nvidia).len(), 12);
        assert!(
            !profiles
                .model_ids(&nvidia)
                .iter()
                .any(|id| id == "deepseek-ai/deepseek-v4.1-flash")
        );
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
                Some("nvidia/nemotron-3.5-super-vl-120b-a12b"),
                Some("fixture"),
            )
            .unwrap();
        assert_eq!(headers["nvcf-poll-seconds"], "3600");
        let untrusted = profiles
            .request_headers(
                &nvidia,
                "https://relay.example/v1/chat/completions",
                Some("nvidia/nemotron-3.5-super-vl-120b-a12b"),
                None,
            )
            .unwrap();
        assert!(untrusted.get("nvcf-poll-seconds").is_none());
        let configured = Profiles::parse(&json!({"providers":{"nvidia":{
            "baseUrl":"https://relay.example/v1",
            "modelOverrides":{"nvidia/nemotron-3.5-super-vl-120b-a12b":{"headers":{"X-Relay-Secret":"private"}}}
        }}})).unwrap();
        let official = configured
            .request_headers(
                &nvidia,
                endpoint,
                Some("nvidia/nemotron-3.5-super-vl-120b-a12b"),
                None,
            )
            .unwrap();
        assert_eq!(official["nvcf-poll-seconds"], "3600");
        assert!(official.get("x-relay-secret").is_none());
        let relay = configured
            .request_headers(
                &nvidia,
                "https://relay.example/v1/chat/completions",
                Some("nvidia/nemotron-3.5-super-vl-120b-a12b"),
                None,
            )
            .unwrap();
        assert_eq!(relay["x-relay-secret"], "private");
    }

    #[test]
    fn kimi_k31_pro_catalog_keeps_host_specific_limits_and_compatibility() {
        let profiles = Profiles::default();
        let model_id = "moonshotai/Kimi-K3.1-Pro";
        for (provider_id, total, max_output) in
            [("baseten", 15, 262_144), ("together", 16, 1_048_576)]
        {
            let provider = profiles.provider(provider_id).unwrap();
            assert_eq!(profiles.model_ids(&provider).len(), total);
            assert_eq!(
                profiles.model_adapter(&provider, model_id),
                Adapter::ChatCompletions
            );
            let model = profiles.model(&provider, model_id).unwrap();
            assert_eq!(model.context_window, Some(1_048_576));
            assert_eq!(model.max_tokens, Some(max_output));
            assert_eq!(model.supports_images(), Some(true));
            assert_eq!(model.image_resize().unwrap().max_width, Some(2000));
            assert_ne!(
                profiles
                    .chat_compatibility(&provider, Some(&model))
                    .supports_developer_role(),
                Some(true)
            );
        }
        let together = profiles.provider("together").unwrap();
        let previous = profiles.model(&together, "moonshotai/Kimi-K3").unwrap();
        assert_eq!(previous.max_tokens, Some(1_048_576));
        let latest = profiles.model(&together, model_id).unwrap();
        assert_eq!(latest.mapped_effort("medium"), Some(None));
    }

    #[test]
    fn huggingface_catalog_uses_text_and_vision_metadata_from_the_router() {
        let profiles = Profiles::default();
        let provider = profiles.provider("huggingface").unwrap();
        assert_eq!(profiles.model_ids(&provider).len(), 87);
        let model = profiles
            .model(&provider, "moonshotai/Kimi-K3.1-Pro")
            .unwrap();
        assert_eq!(model.supports_images(), Some(true));
        assert_eq!(
            profiles
                .chat_compatibility(&provider, Some(&model))
                .supports_developer_role(),
            None
        );
        assert_eq!(model.image_resize().unwrap().max_width, Some(2000));
        let llama = profiles
            .model(&provider, "meta-llama/Llama-3.2-1B-Instruct")
            .unwrap();
        assert_eq!(llama.supports_images(), Some(false));
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
    fn configured_credential_never_follows_model_endpoint_override() {
        for (provider_base, model_base) in [
            ("https://trusted.example/v1", "https://other.example/v1"),
            ("https://other.example/v1", "https://trusted.example/v1"),
        ] {
            let mut args = super::super::Cli::parse_from(["pi"]);
            args.model_profiles = Profiles::parse(&json!({"providers":{"relay":{
                "api":"openai-completions", "baseUrl":provider_base,
                "apiKey":"provider-key", "models":[{"id":"override", "baseUrl":model_base}]
            }}}))
            .unwrap();
            let provider = args.model_profiles.provider("relay").unwrap();
            let primary = args.model_profiles.endpoint(&provider).to_owned();
            let overridden = args
                .model_profiles
                .model_endpoint(&provider, "override")
                .unwrap()
                .to_owned();
            for saved in [
                json!({}),
                json!({"relay":{"type":"api_key", "key":"saved-key", "env":{"SECRET":"value"}}}),
                json!({"relay":{"type":"api_key", "env":{"SECRET":"value"}}}),
            ] {
                args.model_keys = super::super::model_auth::Keys::parse(&saved);
                let expected = if saved["relay"]["key"].is_string() {
                    "saved-key"
                } else {
                    "provider-key"
                };
                assert_eq!(
                    args.configured_model_credential(&provider, &primary)
                        .unwrap()
                        .literal(),
                    Some(expected)
                );
                assert!(
                    args.configured_model_credential(&provider, &overridden)
                        .is_none()
                );
            }
            args.model_keys = super::super::model_auth::Keys::parse(&json!({"relay":{
                "type":"api_key", "key":"explicit-key", "endpoint":overridden
            }}));
            assert_eq!(
                args.configured_model_credential(&provider, &primary)
                    .unwrap()
                    .literal(),
                Some("provider-key")
            );
            assert_eq!(
                args.configured_model_credential(&provider, &overridden)
                    .unwrap()
                    .literal(),
                Some("explicit-key")
            );
        }
    }

    #[tokio::test]
    async fn provider_base_derived_api_routes_share_credentials_and_environment() {
        let mut args = super::super::Cli::parse_from(["pi"]);
        args.model_profiles = Profiles::parse(&json!({"providers":{"relay":{
            "baseUrl":"https://relay.example/v1", "apiKey":"profile-key",
            "headers":{"X-Secret":"$PI_RS_DERIVED_ROUTE_SECRET_4C87"},
            "models":[
                {"id":"chat", "api":"openai-completions"},
                {"id":"messages", "api":"anthropic-messages"},
                {"id":"other", "api":"anthropic-messages", "baseUrl":"https://other.example/v1"}
            ]
        }}}))
        .unwrap();
        let provider = args.model_profiles.provider("relay").unwrap();
        let chat = args.model_profiles.endpoint(&provider).to_owned();
        let messages = args
            .model_profiles
            .model_endpoint(&provider, "messages")
            .unwrap()
            .to_owned();
        let other = args
            .model_profiles
            .model_endpoint(&provider, "other")
            .unwrap()
            .to_owned();
        assert_eq!(chat, "https://relay.example/v1/chat/completions");
        assert_eq!(messages, "https://relay.example/v1/messages");
        for endpoint in [&chat, &messages] {
            assert_eq!(
                args.configured_model_credential(&provider, endpoint)
                    .unwrap()
                    .literal(),
                Some("profile-key")
            );
        }
        assert!(
            args.configured_model_credential(&provider, &other)
                .is_none()
        );

        args.model_keys = super::super::model_auth::Keys::parse(&json!({"relay":{
            "type":"api_key", "key":"saved-key", "env":{"PI_RS_DERIVED_ROUTE_SECRET_4C87":"saved-header"}
        }}));
        for (model, endpoint) in [("chat", &chat), ("messages", &messages)] {
            assert_eq!(
                args.configured_model_credential(&provider, endpoint)
                    .unwrap()
                    .literal(),
                Some("saved-key")
            );
            let headers = args
                .model_profiles
                .header_settings(&provider, endpoint, Some(model))
                .with_environment(args.model_keys.environment(
                    &provider,
                    endpoint,
                    &args.model_profiles,
                ));
            assert_eq!(
                headers
                    .static_headers(&provider, Some("saved-key"))
                    .unwrap()["x-secret"],
                "saved-header"
            );
            assert_eq!(
                headers.resolve(&provider, Some("saved-key")).await.unwrap()["x-secret"],
                "saved-header"
            );
        }
        assert!(
            args.configured_model_credential(&provider, &other)
                .is_none()
        );
        assert!(
            args.model_keys
                .environment(&provider, &other, &args.model_profiles)
                .is_none()
        );
        let headers = args
            .model_profiles
            .header_settings(&provider, &other, Some("other"))
            .with_environment(
                args.model_keys
                    .environment(&provider, &other, &args.model_profiles),
            );
        assert!(headers.static_headers(&provider, None).is_err());
    }

    #[test]
    fn builtin_official_messages_routes_prefer_configured_key() {
        const CHILD: &str = "PI_RS_TEST_OFFICIAL_ROUTE_CHILD_6D23";
        if std::env::var_os(CHILD).is_none() {
            let output = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "model_profiles::tests::builtin_official_messages_routes_prefer_configured_key",
                ])
                .env(CHILD, "1")
                .env("OPENROUTER_API_KEY", "ambient-openrouter")
                .env("FIREWORKS_API_KEY", "ambient-fireworks")
                .output()
                .unwrap();
            assert!(
                output.status.success(),
                "{}\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed"));
            return;
        }
        for (id, messages, ambient) in [
            (
                "openrouter",
                "https://openrouter.ai/api/v1/messages",
                "ambient-openrouter",
            ),
            (
                "fireworks",
                "https://api.fireworks.ai/inference/v1/messages",
                "ambient-fireworks",
            ),
        ] {
            let catalogue_profile =
                Profiles::parse(&json!({"providers":{id:{"apiKey":"catalogue-key"}}})).unwrap();
            let catalogue_provider = catalogue_profile.provider(id).unwrap();
            assert_eq!(
                catalogue_profile
                    .credential(&catalogue_provider, messages)
                    .unwrap()
                    .literal(),
                Some("catalogue-key")
            );
            let mut args = super::super::Cli::parse_from(["pi"]);
            args.model_profiles = Profiles::parse(&json!({"providers":{id:{
                "apiKey":"configured-key", "models":[
                    {"id":"messages-test", "api":"anthropic-messages"},
                    {"id":"other-test", "api":"anthropic-messages", "baseUrl":"https://other.example/v1"}
                ]
            }}})).unwrap();
            let provider = args.model_profiles.provider(id).unwrap();
            let official = args
                .model_profiles
                .model_endpoint(&provider, "messages-test")
                .unwrap();
            let other = args
                .model_profiles
                .model_endpoint(&provider, "other-test")
                .unwrap();
            assert_eq!(official, messages);
            assert_eq!(
                args.configured_model_credential(&provider, official)
                    .unwrap()
                    .literal(),
                Some("configured-key")
            );
            assert!(args.configured_model_credential(&provider, other).is_none());
            args.model_keys = super::super::model_auth::Keys::parse(
                &json!({id:{"type":"api_key", "key":"stored-key"}}),
            );
            assert_eq!(
                args.configured_model_credential(&provider, official)
                    .unwrap()
                    .literal(),
                Some("stored-key")
            );
            assert!(args.configured_model_credential(&provider, other).is_none());
            args.model_keys = super::super::model_auth::Keys::parse(&json!({}));
            let no_key = json!({"providers":{id:{"models":[{"id":"messages-test", "api":"anthropic-messages"}]}}});
            let no_key = Profiles::parse(&no_key).unwrap();
            args.model_profiles = no_key;
            assert_eq!(
                args.configured_model_credential(&provider, messages)
                    .unwrap()
                    .literal(),
                Some(ambient)
            );
            args.model_keys = super::super::model_auth::Keys::parse(
                &json!({id:{"type":"api_key", "key":"stored-key"}}),
            );
            assert_eq!(
                args.configured_model_credential(&provider, messages)
                    .unwrap()
                    .literal(),
                Some("stored-key")
            );
        }
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
        args.restore_model_selection("outside-scope".into(), None, Some("anthropic"), false);
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
    fn mistral_catalog_uses_the_conversations_transport() {
        let profiles = Profiles::default();
        let provider = profiles.provider("mistral").unwrap();
        assert_eq!(provider.adapter(), Adapter::MistralConversations);
        assert_eq!(profiles.model_ids(&provider).len(), 34);
        assert_eq!(profiles.model_ids(&provider)[0], "devstral-medium-latest");
        assert_eq!(
            profiles.endpoint(&provider),
            "https://api.mistral.ai/v1/chat/completions"
        );
        assert_eq!(
            profiles.session_affinity(
                &provider,
                "deepseek-v4-1-flash-1017",
                profiles.endpoint(&provider)
            ),
            ["x-affinity"]
        );
        let model = profiles
            .model(&provider, "deepseek-v4-1-flash-1017")
            .unwrap();
        assert_eq!(model.mapped_effort("high"), Some(Some("high")));
        assert_eq!(model.mapped_effort("off"), Some(Some("none")));
        assert_eq!(model.supports_images(), Some(true));
        assert_eq!(
            model.image_resize(),
            Some(ImageResizeProfile {
                max_width: Some(2000),
                max_height: Some(2000),
                max_bytes: Some(4_718_592),
                jpeg_quality: Some(80),
            })
        );

        let ministral = profiles.model(&provider, "ministral-3b-2512").unwrap();
        assert_eq!(ministral.max_tokens, Some(131_072));

        let large = profiles.model(&provider, "mistral-large-latest").unwrap();
        assert_eq!(large.context_window, Some(1_048_576));
        assert_eq!(large.reasoning, Some(true));
        assert_eq!(large.mapped_effort("medium"), Some(Some("medium")));
        assert_eq!(large.mapped_effort("off"), Some(None));
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

    #[test]
    fn endpointless_saved_credentials_do_not_follow_model_overrides() {
        for models in [
            json!([{ "id":"hostile", "baseUrl":"https://hostile.example/v1" }]),
            json!([{ "id":"normal" }, { "id":"hostile", "baseUrl":"https://hostile.example/v1" }]),
        ] {
            let profiles = Profiles::parse(&json!({"providers":{"relay":{
                "api":"openai-completions", "baseUrl":"https://trusted.example/v1",
                "headers":{"X-Secret":"$PI_RS_TEST_HEADER_SECRET_3614"}, "models":models
            }}}))
            .unwrap();
            let provider = profiles.provider("relay").unwrap();
            let trusted = profiles.endpoint(&provider);
            let hostile = profiles.model_endpoint(&provider, "hostile").unwrap();
            let saved = json!({"relay":{"type":"api_key", "key":"saved-key", "env":{"PI_RS_TEST_HEADER_SECRET_3614":"sensitive"}}});
            let keys = super::super::model_auth::Keys::parse(&saved);
            assert!(keys.environment(&provider, trusted, &profiles).is_some());
            assert!(
                keys.custom_credential(&provider, trusted, &profiles)
                    .is_some()
            );
            assert!(keys.environment(&provider, hostile, &profiles).is_none());
            assert!(
                keys.custom_credential(&provider, hostile, &profiles)
                    .is_none()
            );
            let headers = profiles
                .header_settings(&provider, hostile, Some("hostile"))
                .with_environment(keys.environment(&provider, hostile, &profiles));
            assert!(headers.static_headers(&provider, None).is_err());

            let mut explicit = saved;
            explicit["relay"]["endpoint"] = json!(hostile);
            let keys = super::super::model_auth::Keys::parse(&explicit);
            assert!(keys.environment(&provider, trusted, &profiles).is_none());
            assert!(keys.environment(&provider, hostile, &profiles).is_some());
            assert_eq!(
                keys.credential(&provider, hostile).unwrap().literal(),
                Some("saved-key")
            );
            let headers = profiles
                .header_settings(&provider, hostile, Some("hostile"))
                .with_environment(keys.environment(&provider, hostile, &profiles));
            assert_eq!(
                headers.static_headers(&provider, None).unwrap()["x-secret"],
                "sensitive"
            );
        }
        let profiles = Profiles::parse(&json!({"providers":{"relay":{
            "api":"openai-completions", "baseUrl":"https://trusted.example/v1",
            "models":[{"id":"only", "baseUrl":"https://trusted.example/v1/"}]
        }}}))
        .unwrap();
        let provider = profiles.provider("relay").unwrap();
        let endpoint = profiles.model_endpoint(&provider, "only").unwrap();
        let keys = super::super::model_auth::Keys::parse(&json!({"relay":{
            "type":"api_key", "env":{"HEADER_VALUE":"local"}
        }}));
        assert_eq!(endpoint, profiles.endpoint(&provider));
        assert!(keys.environment(&provider, endpoint, &profiles).is_some());
    }
}
